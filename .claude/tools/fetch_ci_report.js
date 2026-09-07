#!/usr/bin/env node
/**
 * Fetch ClickHouse CI test reports without Playwright
 *
 * Usage:
 *   node fetch_ci_report.js <url> [options]
 *
 * URL formats supported:
 *   - GitHub PR URLs: https://github.com/ClickHouse/ClickHouse/pull/12345 (fetches ALL CI reports)
 *   - HTML URLs: https://s3.amazonaws.com/.../praktika.html?PR=...&sha=...&name_0=...
 *   - Direct JSON URLs: https://s3.amazonaws.com/.../result_*.json
 *
 * Options:
 *   --test <name>    Filter to show only tests matching this name
 *   --failed         Show failed test names in PR summary
 *   --all            Show all test results (not just summary)
 *   --links          Show artifact links
 *   --cidb           Show CIDB links for failed tests
 *   --download-logs [path]  Download logs to given path (default: /tmp/ci_logs.tar.gz or .tar.zst)
 *   --report <number> For PR URLs: fetch only one specific report (default: fetch all)
 *   --credentials <user,password>  HTTP Basic Auth credentials (comma-separated). Only for ClickHouse_private repository
 *
 * Examples:
 *   node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171"
 *   node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --failed --cidb
 *   node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --report 2
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?PR=94537&..."
 *   node fetch_ci_report.js "https://s3.amazonaws.com/.../result_integration_tests.json"
 *   node fetch_ci_report.js "<url>" --test peak_memory --links
 *   node fetch_ci_report.js "<url>" --failed --download-logs
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');
const { execSync, execFileSync } = require('child_process');
const zlib = require('zlib');

/**
 * Normalize task name as done in the HTML page
 */
function normalizeTaskName(name) {
  return name.toLowerCase()
    .replace(/[^a-z0-9]/g, '_')
    .replace(/_+/g, '_')
    .replace(/_+$/, '');
}

/**
 * Fetch a URL and return the response body
 */
// Transparently decompress object-level compression (not HTTP transfer-encoding): CI stores text
// artifacts above a size threshold as zstd (see ci/praktika/s3.py). Detected by magic bytes, so it
// works whether the object is served plain, as `.zst`, or gzip.
function maybeDecompress(buf) {
  if (buf.length >= 4 && buf[0] === 0x28 && buf[1] === 0xb5 && buf[2] === 0x2f && buf[3] === 0xfd) {
    if (typeof zlib.zstdDecompressSync === 'function') return zlib.zstdDecompressSync(buf);
    const { execFileSync } = require('child_process');
    // Bound the child: a wedged zstd would otherwise block the whole helper indefinitely.
    return execFileSync('zstd', ['-dcq'], { input: buf, maxBuffer: 2 * 1024 * 1024 * 1024, timeout: 120000 });
  }
  if (buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b) {
    return zlib.gunzipSync(buf);
  }
  return buf;
}

function fetchUrl(urlString, credentials = null) {
  return new Promise((resolve, reject) => {
    let parsedUrl;
    try {
      parsedUrl = new URL(urlString);
    } catch (e) {
      reject(e);
      return;
    }
    const protocol = parsedUrl.protocol === 'https:' ? https : http;

    const options = {
      method: 'GET',
      headers: {}
    };

    if (credentials) {
      const auth = Buffer.from(`${credentials.username}:${credentials.password}`).toString('base64');
      options.headers['Authorization'] = `Basic ${auth}`;
    }

    const req = protocol.get(urlString, options, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        // Follow redirect
        return fetchUrl(res.headers.location, credentials).then(resolve).catch(reject);
      }

      // A missing/expired plain artifact may exist only as a zstd-compressed sibling: CI
      // compresses text artifacts over a size threshold (see ci/praktika/s3.py).
      if ((res.statusCode === 403 || res.statusCode === 404) && !urlString.endsWith('.zst')) {
        res.resume();
        return fetchUrl(urlString + '.zst', credentials).then(resolve).catch(reject);
      }

      if (res.statusCode === 403) {
        reject(new Error('403 Forbidden - Report does not exist or expired'));
        return;
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
        return;
      }

      // Handle gzip compression
      let stream = res;
      const encoding = res.headers['content-encoding'];
      if (encoding === 'gzip') {
        stream = res.pipe(zlib.createGunzip());
      } else if (encoding === 'deflate') {
        stream = res.pipe(zlib.createInflate());
      }

      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('end', () => {
        // maybeDecompress may throw (missing zstd binary, corrupt archive); a throw from this
        // async handler would escape the Promise as an uncaught exception, so surface it as a
        // normal rejection instead.
        try {
          resolve(maybeDecompress(Buffer.concat(chunks)).toString('utf8'));
        } catch (err) {
          reject(err);
        }
      });
      stream.on('error', reject);
    });

    req.on('error', reject);
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

/**
 * Parse the HTML URL to extract parameters and construct JSON URLs
 */
async function parseReportUrl(htmlUrl, credentials = null) {
  let url;
  try {
    url = new URL(htmlUrl);
  } catch (e) {
    throw new Error(`Invalid report URL: ${e.message}`);
  }
  const params = url.searchParams;

  const PR = params.get('PR');
  const REF = params.get('REF');
  const sha = params.get('sha');
  const base_url = params.get('base_url');

  // Extract name parameters (name_0, name_1, etc.)
  const nameParams = [];
  params.forEach((value, key) => {
    if (key.startsWith('name_')) {
      const index = parseInt(key.split('_')[1], 10);
      nameParams[index] = value;
    }
  });

  // Construct base URL
  let baseUrl = base_url;
  if (!baseUrl) {
    // Default to the S3 bucket path
    baseUrl = url.origin + url.pathname.split('/').slice(0, -1).join('/');
  }

  // Construct suffix
  let suffix = '';
  if (PR) {
    suffix = `PRs/${encodeURIComponent(PR)}`;
  } else if (REF) {
    suffix = `REFs/${encodeURIComponent(REF)}`;
  } else {
    throw new Error('Either PR or REF parameter is required');
  }

  if (!sha) {
    throw new Error('sha parameter is required');
  }

  if (nameParams.length === 0) {
    throw new Error('At least name_0 parameter is required');
  }

  // Resolve sha=latest by fetching commits.json
  let resolvedSha = sha;
  if (sha === 'latest') {
    const commitsUrl = `${baseUrl}/${suffix}/commits.json`;
    const commitsText = await fetchUrl(commitsUrl, credentials);
    let commits;
    try {
      commits = JSON.parse(commitsText);
    } catch (e) {
      throw new Error(`Invalid JSON in commits.json: ${e.message}`);
    }
    if (!commits || commits.length === 0) {
      throw new Error('No commits found in commits.json');
    }
    resolvedSha = commits[commits.length - 1].sha;
  }

  return { baseUrl, suffix, sha: resolvedSha, nameParams };
}

/**
 * Construct JSON URL for a given task name
 */
function constructJsonUrl(baseUrl, suffix, sha, workflowName, taskName) {
  // The S3 layout inserts the normalized workflow name (name_0) as a path segment:
  //   <suffix>/<sha>/<normalized_workflow>/result_<job>.json
  // Both the workflow-index report (result_<workflow>.json) and every per-job report
  // (result_<job>.json) live under that same <normalized_workflow> directory. See
  // fetchData/buildResultPath in ci/praktika/praktika.html for the ground truth.
  const workflowSegment = normalizeTaskName(workflowName);
  const normalizedTask = normalizeTaskName(taskName);
  return `${baseUrl}/${suffix}/${encodeURIComponent(sha)}/${workflowSegment}/result_${normalizedTask}.json`;
}

/**
 * Check if a status represents a failure
 */
/**
 * Extract the meaningful failure reason from result.info.
 *
 * result.info for stateless/functional tests has this structure:
 *   Reason: <category>:
 *   <actual error output, exceptions, or diff>
 *
 *   /path/to/test.sh.debuglog:
 *   + [timestamp] [:line] bash-command ...
 *   + [timestamp] [:line] bash-command ...
 *
 * The actionable part is the top section before the bash xtrace.
 * Showing the tail (as we used to) gives only useless bash commands.
 */
function extractFailureReason(info, maxLines = 40) {
  if (!info) return [];
  const allLines = info.split('\n');
  // Bash debug-trace lines ('+ [timestamp] [line] cmd') are pure noise — strip everything
  // from the first xtrace line (or the '.debuglog:' section header) to the end.
  const sepIdx = allLines.findIndex(
    l => /^\+\+? \[/.test(l) || /\.(?:debug)?log:$/.test(l.trim())
  );
  const meaningful = (sepIdx >= 0 ? allLines.slice(0, sepIdx) : allLines)
    .filter(l => l.trim());
  if (meaningful.length === 0) return [];
  // Short enough to show in full.
  if (meaningful.length <= maxLines) return meaningful;
  // Too long: show head + tail so both 'Reason: ...' (stateless tests, always at top)
  // and build errors / 'ninja: stopped' (always at bottom) are visible.
  const half = Math.floor(maxLines / 2);
  return [
    ...meaningful.slice(0, half),
    `--- (${meaningful.length - maxLines} lines omitted) ---`,
    ...meaningful.slice(-half),
  ];
}

function isFailureStatus(status) {
  return status === 'failed' || status === 'FAIL' || status === 'failure' ||
         status === 'error' || status === 'ERROR';
}

/**
 * Apply ext labels / CIDB links from a result node onto a test object.
 * Shared by the normal leaf path and the synthesized job-level entry.
 */
function applyExtToTest(test, ext) {
  if (!ext) return;
  const byName = new Map();
  const upsert = (name, link) => {
    if (!name) return;
    const prev = byName.get(name) || {};
    byName.set(name, { name, link: link || prev.link });
  };
  if (Array.isArray(ext.labels)) {
    for (const item of ext.labels) {
      if (typeof item === 'string') upsert(item);
      else if (item && typeof item === 'object' && item.name) upsert(item.name, item.link);
    }
  }
  if (Array.isArray(ext.hlabels)) {
    for (const item of ext.hlabels) {
      if (Array.isArray(item) && item[0]) upsert(item[0], item[1]);
    }
  }
  const cidbLinks = [], labels = [];
  for (const { name, link } of byName.values()) {
    if (name === 'cidb') { if (link) cidbLinks.push(link); }
    else labels.push(link ? `${name} (${link})` : name);
  }
  if (cidbLinks.length > 0) test.cidbLinks = cidbLinks;
  if (labels.length > 0) test.labels = labels;
}

/**
 * Parse test results from the JSON data
 */
function parseTestResults(jsonData) {
  const tests = [];

  if (!jsonData || !jsonData.results || jsonData.results.length === 0) {
    // Job-level failure with no child results (e.g. status: "ERROR" for the whole job).
    // Synthesize a single leaf so the top-level info and links are still surfaced.
    if (jsonData && isFailureStatus(jsonData.status)) {
      const test = {
        name: jsonData.name || 'Job',
        status: jsonData.status,
        duration: jsonData.duration || 0,
        jobLevel: true,
      };
      if (jsonData.info) test.info = jsonData.info;
      if (jsonData.links && jsonData.links.length > 0) test.links = jsonData.links;
      applyExtToTest(test, jsonData.ext);
      tests.push(test);
    }
    return tests;
  }

  function extractTests(results, prefix = '') {
    for (const result of results) {
      if (result.results && result.results.length > 0) {
        // Nested results
        extractTests(result.results, prefix ? `${prefix}/${result.name}` : result.name);
      } else {
        // Leaf result - this is a test or build step.
        // Use the leaf's own name, NOT the ancestor-prefixed path: the leaf name is exactly the
        // `checks.test_name` value (e.g. "Server died", "test_dns_cache/test.py::..."), whereas a
        // prefixed form like "Tests/Server died" matches no `checks` row and would break the
        // history query and issue matching in steps 2-3. The `prefix` (grouping nodes such as
        // "Tests") is intra-report context already conveyed by the report header.
        const test = {
          name: result.name,
          status: result.status || 'UNKNOWN',
          duration: result.duration || 0
        };

        // Include info field (failure reason + optional bash debug trace)
        if (result.info) {
          test.info = result.info;
        }

        // Include links from this result
        if (result.links && result.links.length > 0) {
          test.links = result.links;
        }

        // Extract CIDB links and other labels (see applyExtToTest for format details).
        applyExtToTest(test, result.ext);

        tests.push(test);
      }
    }
  }

  extractTests(jsonData.results);

  // The top-level node may itself be FAIL/ERROR even when all leaf results passed
  // (Praktika sets result.set_status(ERROR) in the non-zero-exit path after the subtree
  // is already populated). If no child captures the failure, synthesize one from the
  // top-level node so --failed never prints "Total: 0" for a truly failed job.
  if (isFailureStatus(jsonData.status)) {
    const hasFailing = tests.some(t => isFailureStatus(t.status));
    if (!hasFailing) {
      // No failing leaves — synthesize from root.
      const test = {
        name: jsonData.name || 'Job',
        status: jsonData.status,
        duration: jsonData.duration || 0,
        jobLevel: true,
      };
      if (jsonData.info) test.info = jsonData.info;
      if (jsonData.links && jsonData.links.length > 0) test.links = jsonData.links;
      applyExtToTest(test, jsonData.ext);
      tests.push(test);
    } else if (
      jsonData.info && jsonData.info.trim() &&
      !tests.some(t => t.info === jsonData.info) &&
      !/^Failures:\s/.test(jsonData.info) &&
      !/^Failed:\s/.test(jsonData.info)
    ) {
      // Root has additional error context (e.g. "Test execution was interrupted") not captured
      // by any failing leaf. Synthesize an aggregate entry so the interrupt context is visible
      // alongside the normal test failures.
      const test = {
        name: '[job error]',
        status: jsonData.status,
        duration: jsonData.duration || 0,
        info: jsonData.info,
        jobLevel: true,
      };
      if (jsonData.links && jsonData.links.length > 0) test.links = jsonData.links;
      applyExtToTest(test, jsonData.ext);
      tests.push(test);
    }
  }

  return tests;
}

/**
 * Extract artifact links from JSON data
 */
function extractArtifactLinks(jsonData) {
  const links = [];

  if (!jsonData) {
    return links;
  }

  // Extract links from the top-level links array
  if (jsonData.links) {
    for (const link of jsonData.links) {
      if (typeof link === 'string') {
        links.push({ text: link.split('/').pop(), href: link });
      }
    }
  }

  // Extract links from results
  function extractFromResults(results) {
    if (!results) return;

    for (const result of results) {
      if (result.links) {
        for (const link of result.links) {
          if (typeof link === 'string') {
            links.push({ text: link.split('/').pop(), href: link });
          }
        }
      }
      if (result.results) {
        extractFromResults(result.results);
      }
    }
  }

  extractFromResults(jsonData.results);

  // Filter to artifact/log links and the ClickHouse binary; exclude praktika.html/json.html navigation links.
  return links.filter(link => {
    const h = link.href;
    const name = h.split('/').pop();
    // Exclude CI navigation/report links
    if (h.includes('json.html') || h.includes('praktika.html')) return false;
    // ClickHouse binary: starts with 'clickhouse' and has no dots (no file extension).
    // Matches 'clickhouse', 'clickhouse-stripped', etc. but not log/config files.
    if (name.startsWith('clickhouse') && !name.includes('.')) return true;
    // Installable packages
    if (name.endsWith('.deb') || name.endsWith('.rpm')) return true;
    // Log and archive formats
    if (h.includes('.log') || h.includes('.log.zst')) return true;
    if (h.includes('.tar.gz') || h.includes('.tar.zst') || h.includes('.tgz')) return true;
    if (h.includes('.zst')) return true;
    if (h.includes('.html') && !h.includes('json.html') && !h.includes('praktika.html')) return true;
    if (h.includes('.tsv')) return true;
    if (h.includes('configs')) return true;
    if (h.includes('artifact_report')) return true;
    return false;
  });
}

/**
 * Extract CI report URLs from a GitHub PR
 */
async function getCIReportsFromPR(prUrl) {
  // Parse PR number from URL
  const match = prUrl.match(/github\.com\/ClickHouse\/ClickHouse\/pull\/(\d+)/);
  if (!match) {
    throw new Error('Invalid GitHub PR URL format');
  }
  const prNumber = match[1];

  console.log(`Fetching CI reports for PR #${prNumber}...\n`);

  // Fetch PR comments to find CI bot comment.
  // Drop GH_CONFIG_DIR before spawning gh: some agent/runner checkouts set it to a poisoned
  // config dir (no/expired auth) that makes `gh api` fail, while the default config is fine.
  // Other repo tooling (patch-release-check) does the same via `env -u GH_CONFIG_DIR gh`.
  const ghEnv = { ...process.env };
  delete ghEnv.GH_CONFIG_DIR;
  try {
    const commentsJson = execSync(`gh api repos/ClickHouse/ClickHouse/issues/${prNumber}/comments --paginate --jq '.[] | select(.user.login == "clickhouse-gh[bot]") | {body, created_at}'`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      env: ghEnv
    });

    const comments = commentsJson.trim().split('\n').filter(l => l.trim()).map(l => JSON.parse(l));
    comments.sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
    if (!comments || comments.length === 0) {
      throw new Error('No CI bot comment found');
    }

    // Search through all bot comments for CI report URLs (not just the latest). Exclude backtick and
    // quote chars so a URL quoted in markdown (e.g. inside the AI-review text) is not captured with
    // trailing junk, strip trailing punctuation, and dedupe -- otherwise the same report is fetched
    // twice and the summary is doubled.
    const reportUrlPattern = /https:\/\/s3\.amazonaws\.com\/clickhouse(?:-private)?-test-reports\/(?:praktika|json)\.html\?[^\s)`'"]+/g;
    for (const comment of comments) {
      if (!comment.body) continue;
      let urls = comment.body.match(reportUrlPattern);
      if (urls && urls.length > 0) {
        urls = urls.map(u => u.replace(/[.,;]+$/, ''));
        return [...new Set(urls)];
      }
    }

    throw new Error('No CI report URLs found in bot comments');
  } catch (error) {
    if (error.message.includes('No CI bot comment found') || error.message.includes('No CI report URLs found')) {
      throw error;
    }
    throw new Error(`Failed to fetch PR comments: ${error.message}`);
  }
}

/**
 * Fetch and parse the CI report
 */
/**
 * Given a top-level index report URL (praktika.html?...&name_0=X, no name_1) and its raw JSON, return
 * the per-job report URLs (praktika.html?...&name_1=<job>) for the FAILED jobs. Child names come from
 * the report's IMMEDIATE children (the job/check rows) -- never from flattened leaf tests, whose
 * names (e.g. "Server died") are not valid name_1 job identifiers.
 */
function childReportUrlsForFailedJobs(topLevelUrl, jsonData) {
  const jobs = (jsonData && Array.isArray(jsonData.results)) ? jsonData.results : [];
  return jobs
    .filter(j => isFailureStatus(j.status))
    .map(j => topLevelUrl.replace(/&name_1=[^&]*/, '') + `&name_1=${encodeURIComponent(j.name)}`);
}

/**
 * Return the per-job report URL for every job in a workflow-index, regardless of status.
 * Used by --report N to let the user pick any concrete job from a workflow-index URL.
 */
function allChildReportUrls(topLevelUrl, jsonData) {
  const jobs = (jsonData && Array.isArray(jsonData.results)) ? jsonData.results : [];
  return jobs.map(j => topLevelUrl.replace(/&name_1=[^&]*/, '') + `&name_1=${encodeURIComponent(j.name)}`);
}

/**
 * Return true when a URL is a concrete job report (name_1 present, or name_0 is not a
 * workflow-level aggregator). Workflow-index URLs (name_0=PR|MasterCI|REF|master with no
 * name_1) return false so they can be filtered out before --report N selection.
 */
function isConcreteJobUrl(u) {
  const m = u.match(/[?&]name_0=([^&]+)/);
  if (!m) return true;
  const name0 = decodeURIComponent(m[1]);
  return !/^(PR|MasterCI|REF|master)$/i.test(name0) || /[?&]name_1=/.test(u);
}

/**
 * Render a set of report URLs as a multi-report summary (one row per report, failures under each).
 * Shared by the GitHub-PR path and the direct top-level-index path.
 */
async function renderMultiReport(ciUrls, options) {
  // --binary only works for a single concrete build report (name_1=Build (...)).
  // PR and top-level index URLs fan out here and never reach the binary-download block.
  if (options.binary) {
    process.stderr.write(
      'Error: --binary requires a concrete build report URL, e.g.:\n' +
      '  ...praktika.html?PR=...&sha=...&name_0=PR&name_1=Build%20(amd_binary)\n' +
      'PR URLs and top-level index URLs do not carry binary artifacts.\n'
    );
    process.exit(1);
  }
  console.log(`Fetching all reports...\n`);
  const allResults = [];

  for (let i = 0; i < ciUrls.length; i++) {
    const url = ciUrls[i];
    const nameMatch = url.match(/name_0=([^&]+)/);
    const name1Match = url.match(/name_1=([^&]+)/);
    const jobName = nameMatch ? decodeURIComponent(nameMatch[1]) : 'Unknown';
    const subJobName = name1Match ? decodeURIComponent(name1Match[1]) : null;
    const fullJobName = subJobName ? `${jobName} -> ${subJobName}` : jobName;

    try {
      console.log(`[${i + 1}/${ciUrls.length}] ${fullJobName}...`);
      const result = await fetchReport(url, { ...options, isSingleReport: true });
      allResults.push({
        index: i + 1,
        jobName: fullJobName,
        url,
        isPRLevel: !subJobName, // true if this is a PR-level report (no name_1)
        ...result
      });
    } catch (error) {
      console.log(`  Error: ${error.message}\n`);
      allResults.push({
        index: i + 1,
        jobName: fullJobName,
        url,
        isPRLevel: !subJobName,
        error: error.message
      });
    }
  }

  // Print summary
  console.log('\n' + '='.repeat(80));
  console.log('CI REPORTS SUMMARY');
  console.log('='.repeat(80) + '\n');

  let totalTests = 0;
  let totalPassed = 0;
  let totalFailed = 0;
  let totalSkipped = 0;

  // Per-failure detail is normally suppressed for the top-level PR report to avoid
  // duplicating the nested per-job reports. But when the PR report is the ONLY report
  // discovered (the bot comment exposed no nested job URLs), suppressing it would leave a
  // PR URL with no failed leaves at all — so show them in that case.
  const onlyPRLevel = allResults.every(r => r.error || r.isPRLevel);

  for (const result of allResults) {
    if (result.error) {
      console.log(`[${result.index}] ${result.jobName}`);
      console.log(`    ❌ Error: ${result.error}\n`);
      continue;
    }

    const { testResults = [] } = result;
    const failed = testResults.filter(t => isFailureStatus(t.status));
    const passed = testResults.filter(t => t.status === 'success' || t.status === 'OK');
    const skipped = testResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

    // Don't let the top-level PR report contribute to the totals when nested per-job reports
    // are also present — it aggregates the same failures, so counting both double-counts.
    // (When the PR report is the only one, onlyPRLevel is true and it does count.)
    if (!result.isPRLevel || onlyPRLevel) {
      totalTests += testResults.length;
      totalPassed += passed.length;
      totalFailed += failed.length;
      totalSkipped += skipped.length;
    }

    const status = failed.length > 0 ? '❌' : '✅';
    console.log(`[${result.index}] ${status} ${result.jobName}`);
    console.log(`    Total: ${testResults.length} | ✅ Passed: ${passed.length} | ❌ Failed: ${failed.length} | ⏭️  Skipped: ${skipped.length}`);

    // For reports with failures, show the HTML link (also for the PR report when it's the
    // only one, so the investigator can drill in).
    if ((!result.isPRLevel || onlyPRLevel) && failed.length > 0 && result.url) {
      console.log(`    🔗 Report: ${result.url}`);
    }

    // Show individual failures for nested reports; for the PR-level report only when it is
    // the sole report (otherwise skip it to avoid duplicating the nested job reports).
    if (failed.length > 0 && options.failedOnly && (!result.isPRLevel || onlyPRLevel)) {
      for (const test of failed) {
        console.log(test.jobLevel ? `      ⚙️ JOB: ${test.name}` : `      ❌ FAIL: ${test.name}`);
        if (test.labels && test.labels.length > 0) {
          console.log(`         🏷️  labels: ${test.labels.join(', ')}`);
        }
        if (options.showCidb && test.cidbLinks && test.cidbLinks.length > 0) {
          for (const cidbLink of test.cidbLinks) {
            console.log(`         📊 CIDB: ${cidbLink}`);
          }
        }
        if (test.links && test.links.length > 0) {
          for (const link of test.links) {
            console.log(`         🔗 ${link}`);
          }
        }
        if (test.info) {
          const reason = extractFailureReason(test.info);
          console.log('         --- failure reason ---');
          for (const line of reason) {
            console.log(`         ${line}`);
          }
          console.log('         --- end ---');
        }
      }
    }
    console.log();
  }

  console.log('='.repeat(80));
  console.log(`TOTAL: ${totalTests} tests | ✅ ${totalPassed} passed | ❌ ${totalFailed} failed | ⏭️  ${totalSkipped} skipped`);
  console.log('='.repeat(80) + '\n');

  return { allResults, summary: { totalTests, totalPassed, totalFailed, totalSkipped } };
}

async function fetchReport(inputUrl, options = {}) {
  try {
    if (!options.isSingleReport) {
      console.log(`Parsing URL: ${inputUrl}\n`);
    }

    let jsonData, targetData;

    // Check if this is a GitHub PR URL
    const isGitHubPR = inputUrl.includes('github.com') && inputUrl.includes('/pull/');

    if (isGitHubPR) {
      // GitHub PR URL - extract CI report URLs
      const ciUrls = await getCIReportsFromPR(inputUrl);

      // If the bot comment exposed only the top-level `PR` report (name_0=PR, no name_1), treat it
      // as an INDEX: its leaves are job/check names, not test cases, so descend into each FAILED
      // job by synthesizing its per-job report URL (praktika.html?...&name_1=<job>, the same form the
      // loop below already fetches). Without this, a failing PR URL would yield only failed job
      // names -- no test names, labels, or CIDB links for steps 2-3.
      const hasNested = ciUrls.some(u => /[?&]name_1=/.test(u));
      const topLevelUrl = ciUrls.find(u => /[?&]name_0=/.test(u) && !/[?&]name_1=/.test(u));
      if (!hasNested && topLevelUrl) {
        try {
          const top = await fetchReport(topLevelUrl, { ...options, isSingleReport: true });
          const childUrls = childReportUrlsForFailedJobs(topLevelUrl, top.jsonData);
          for (const childUrl of childUrls) {
            if (!ciUrls.includes(childUrl)) ciUrls.push(childUrl);
          }
          if (childUrls.length > 0) {
            console.log(`Top-level PR report is an index — descending into ${childUrls.length} failed job report(s).`);
          } else {
            // No failures (all-green PR): expand to ALL concrete children so the display list
            // and --report N indices are consistent with each other.
            const allChildren = allChildReportUrls(topLevelUrl, top.jsonData).filter(isConcreteJobUrl);
            if (allChildren.length > 0) {
              for (const childUrl of allChildren) {
                if (!ciUrls.includes(childUrl)) ciUrls.push(childUrl);
              }
              console.log(`Top-level PR report is all-green — expanded into ${allChildren.length} concrete job report(s).`);
            }
          }
        } catch (e) {
          console.log(`Note: could not expand the top-level PR report into job reports (${e.message}); showing job-level failures only.`);
        }
      }

      // Remove workflow-index entries (name_0=PR|MasterCI|REF|master with no name_1) so that
      // --report N and the displayed numbering only count concrete job reports, not the
      // top-level aggregation URL that would be mishandled as a concrete job when selected.
      const concreteUrls = ciUrls.filter(isConcreteJobUrl);
      if (concreteUrls.length > 0) ciUrls.splice(0, ciUrls.length, ...concreteUrls);

      console.log(`Found ${ciUrls.length} CI report(s)\n`);

      // If a specific report is requested, fetch only that one
      if (options.reportIndex) {
        const idx = parseInt(options.reportIndex) - 1;
        if (idx < 0 || idx >= ciUrls.length) {
          throw new Error(`Invalid report index. Choose 1-${ciUrls.length}`);
        }
        console.log(`Fetching report #${options.reportIndex}...\n`);
        inputUrl = ciUrls[idx];
      } else {
        return await renderMultiReport(ciUrls, options);
      }
    }

    // A direct top-level workflow result JSON (result_pr.json / result_masterci.json / result_ref.json)
    // is a workflow index, just like the praktika.html?...&name_0=PR form. In the S3 layout it lives
    // under the normalized-workflow directory: <sha>/<workflow>/result_<workflow>.json, where the
    // directory name equals the file's workflow token (pr/masterci/ref). Rewrite it to the HTML form
    // so the index handling below applies uniformly (expand into per-job reports; refuse per-job
    // --download-logs) instead of treating the whole PR/workflow as a single job. Concrete job reports
    // are result_<job>.json under the same directory, so they never match this and stay on the
    // single-report path. Setting name_0 to the captured workflow token reconstructs the same
    // <workflow> directory segment via constructJsonUrl.
    const topJson = inputUrl.match(/\/(?:PRs\/(\d+)|REFs\/([^/]+))\/([0-9a-f]{40})\/(?:[^/?]+\/)?result_(pr|masterci|ref)\.json(?:$|\?)/i);
    if (topJson) {
      const prefix = inputUrl.slice(0, topJson.index);
      const workflowToken = topJson[4].toLowerCase();
      inputUrl = topJson[1]
        ? `${prefix}/praktika.html?PR=${topJson[1]}&sha=${topJson[3]}&name_0=${workflowToken}`
        : `${prefix}/praktika.html?REF=${encodeURIComponent(topJson[2])}&sha=${topJson[3]}&name_0=${workflowToken}`;
    }

    // Check if this is a direct JSON URL or an HTML URL with parameters
    const isDirectJsonUrl = inputUrl.includes('.json') || !inputUrl.includes('?');

    if (isDirectJsonUrl) {
      // Direct JSON URL - fetch it directly
      if (!options.isSingleReport) {
        console.log(`Fetching JSON directly: ${inputUrl}\n`);
        // Extract SHA from the URL path (e.g. .../PRs/<pr>/<40-hex-sha>/result_*.json)
        // and print it for consistency with the HTML-URL path.
        const shaInPath = inputUrl.match(/\/([0-9a-f]{40})\//i);
        if (shaInPath) {
          console.log(`SHA: ${shaInPath[1]}\n`);
        }
      }
      const jsonText = await fetchUrl(inputUrl, options.credentials);
      jsonData = JSON.parse(jsonText);
      targetData = jsonData;
    } else {
      // HTML URL with parameters - parse and construct JSON URLs
      const { baseUrl, suffix, sha, nameParams } = await parseReportUrl(inputUrl, options.credentials);

      if (!options.isSingleReport) {
        console.log(`Task: ${nameParams.join(' -> ')}`);
        console.log(`SHA: ${sha}\n`);
      }

      // Construct JSON URL for the primary task (name_0)
      const jsonUrl = constructJsonUrl(baseUrl, suffix, sha, nameParams[0], nameParams[0]);
      if (!options.isSingleReport) {
        console.log(`Fetching JSON: ${jsonUrl}\n`);
      }

      // A workflow-index URL (name_0 is the WORKFLOW — PR / MasterCI / REF — with no name_1)
      // aggregates every job; it is NOT a single job report. Treat it like the PR-URL path: refuse
      // per-job operations (they would act on the wrong job), and otherwise expand into the failed
      // jobs' per-job reports. A concrete single-job URL also has one nameParam but its name_0 is the
      // JOB (e.g. name_0=Stateless tests (...)) — those must stay on the single-report path below,
      // so gate on the workflow name, not merely nameParams.length.
      const isWorkflowIndex = /^(PR|MasterCI|REF|master)$/i.test(nameParams[0]);
      if (isWorkflowIndex && nameParams.length === 1 && !options.isSingleReport) {
        const topJson = JSON.parse(await fetchUrl(jsonUrl, options.credentials));

        // Build the display list once — shared by both the summary and --report N selection.
        // Use failed concrete children only (UX: show what matters). Fall back to all concrete
        // children when there are no failures (all-passed scenario) so --report N still works.
        const failedChildren = childReportUrlsForFailedJobs(inputUrl, topJson);
        const allConcreteChildren = allChildReportUrls(inputUrl, topJson).filter(isConcreteJobUrl);
        const displayList = failedChildren.length > 0 ? failedChildren : allConcreteChildren;

        if (options.reportIndex) {
          const idx = parseInt(options.reportIndex) - 1;
          if (idx < 0 || idx >= displayList.length) {
            throw new Error(`Invalid report index. Choose 1-${displayList.length}`);
          }
          console.log(`Found ${displayList.length} CI report(s)\n`);
          console.log(`Fetching report #${options.reportIndex}...\n`);
          return await fetchReport(displayList[idx], { ...options, reportIndex: undefined });
        }

        if (options.downloadLogs) {
          const failedNames = (topJson.results || []).filter(j => isFailureStatus(j.status)).map(j => j.name);
          throw new Error(
            `'${nameParams[0]}' is a top-level index report, not a single job — --download-logs would ` +
            `fetch the wrong job's artifacts. Re-run against a concrete job report by appending ` +
            `&name_1=<job>. Failed jobs: ${failedNames.join(', ') || '(none)'}.`
          );
        }
        const listDesc = failedChildren.length > 0 ? `${displayList.length} failed` : `all ${displayList.length}`;
        console.log(`Top-level '${nameParams[0]}' index — expanding into ${listDesc} job report(s).\n`);
        return await renderMultiReport(displayList, options);
      }

      // Fetch name_0 JSON data, and name_1 separately if present (matching praktika.html behavior)
      const fetchTasks = [fetchUrl(jsonUrl, options.credentials)];
      if (nameParams.length > 1) {
        const json1Url = constructJsonUrl(baseUrl, suffix, sha, nameParams[0], nameParams[1]);
        if (!options.isSingleReport) {
          console.log(`Fetching JSON (name_1): ${json1Url}\n`);
        }
        fetchTasks.push(fetchUrl(json1Url, options.credentials).catch(() => null));
      }

      const fetchResults = await Promise.all(fetchTasks);
      jsonData = JSON.parse(fetchResults[0]);

      // Resolve target data: use dedicated name_1 JSON if available, fall back to navigating name_0.results
      targetData = jsonData;
      if (nameParams.length > 1) {
        const json1Text = fetchResults[1];
        if (json1Text) {
          targetData = JSON.parse(json1Text);
        } else if (jsonData.results) {
          // Fallback: navigate name_0.results
          const found = jsonData.results.find(r => r.name === nameParams[1]);
          if (!found) {
            throw new Error(`Task not found: ${nameParams[1]}`);
          }
          targetData = found;
        }
        // Resolve deeper names (name_2+) by walking results
        for (let i = 2; i < nameParams.length; i++) {
          if (!targetData.results) {
            throw new Error(`Task not found: ${nameParams[i]}`);
          }
          const found = targetData.results.find(r => r.name === nameParams[i]);
          if (!found) {
            throw new Error(`Task not found: ${nameParams[i]}`);
          }
          targetData = found;
        }
      }
    }

    // Parse test results
    const testResults = parseTestResults(targetData);

    // Extract artifact links from targetData (specific task) rather than jsonData (entire PR)
    const artifactLinks = extractArtifactLinks(targetData);

    // Apply filters (but keep original results for summary)
    let filteredResults = testResults;

    if (options.testFilter) {
      filteredResults = filteredResults.filter(t =>
        t.name.toLowerCase().includes(options.testFilter.toLowerCase())
      );
    }

    // For multi-report mode, don't filter by failed here - we'll show all in summary
    if (options.failedOnly && !options.isSingleReport) {
      filteredResults = filteredResults.filter(t =>
        isFailureStatus(t.status)
      );
    }

    // If this is a single report being fetched (part of multi-report fetch), just return data
    if (options.isSingleReport) {
      return { testResults, artifactLinks, jsonData };
    }

    // When --binary is requested, print only the binary URL to stdout and exit.
    if (options.binary) {
      const binaryLinks = artifactLinks.filter(l => {
        const name = l.href.split('/').pop();
        return (name.startsWith('clickhouse') && !name.includes('.')) ||
               name.endsWith('.deb') || name.endsWith('.rpm');
      });
      if (binaryLinks.length > 0) {
        for (const l of binaryLinks) process.stdout.write(l.href + '\n');
      } else {
        process.stderr.write(
          'No binary artifacts found in this report.\n' +
          '--binary only works with a concrete Build (...) report URL.\n' +
          'If you have a test-job report, replace name_1=<test-job> with\n' +
          'name_1=Build%20(amd_binary) (or the appropriate build variant).\n'
        );
        process.exit(1);
      }
      return { testResults, artifactLinks, jsonData };
    }

    // Print results for standalone report
    console.log('=== Test Results ===\n');

    const failed = filteredResults.filter(t => isFailureStatus(t.status));
    const passed = filteredResults.filter(t => t.status === 'success' || t.status === 'OK');
    const skipped = filteredResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

    console.log(`Total: ${filteredResults.length} | ✅ Passed: ${passed.length} | ❌ Failed: ${failed.length} | ⏭️  Skipped: ${skipped.length}\n`);

    if (failed.length > 0) {
      console.log('--- Failures ---');
      for (const test of failed) {
        console.log(test.jobLevel ? `⚙️ JOB  ${test.name}  (${test.duration}s)` : `❌ FAIL  ${test.name}  (${test.duration}s)`);
        if (test.labels && test.labels.length > 0) {
          console.log(`   🏷️  labels: ${test.labels.join(', ')}`);
        }
        if (options.showCidb && test.cidbLinks && test.cidbLinks.length > 0) {
          for (const cidbLink of test.cidbLinks) {
            console.log(`   📊 CIDB: ${cidbLink}`);
          }
        }
        if (test.links && test.links.length > 0) {
          for (const link of test.links) {
            console.log(`   🔗 ${link}`);
          }
        }
        if (test.info) {
          const reason = extractFailureReason(test.info);
          console.log('   --- failure reason ---');
          for (const line of reason) {
            console.log(`   ${line}`);
          }
          console.log('   --- end ---');
        }
      }
      console.log('');
    }

    if (options.showAll && !options.failedOnly) {
      console.log('--- All Tests ---');
      for (const test of filteredResults) {
        const statusLabel = test.status.toUpperCase().padEnd(8);
        console.log(`${statusLabel} ${test.name}  (${test.duration}s)`);
      }
    }

    if (options.showLinks) {
      console.log('\n=== Artifact Links ===');
      if (artifactLinks.length > 0) {
        for (const link of artifactLinks) {
          console.log(`${link.text}: ${link.href}`);
        }
      } else {
        console.log('No artifact links found');
      }
    }

    // Download logs if requested
    if (options.downloadLogs) {
      const logsLink = artifactLinks.find(l => l.href.includes('logs.tar.gz') || l.href.includes('logs.tar.zst'));
      if (logsLink) {
        console.log(`\nDownloading logs from: ${logsLink.href}`);
        const ext = logsLink.href.endsWith('.zst') ? '.tar.zst' : '.tar.gz';
        const logsPath = options.downloadLogs !== true ? options.downloadLogs : `/tmp/ci_logs${ext}`;
        execFileSync('curl', ['-sL', logsLink.href, '-o', logsPath]);
        console.log(`Logs saved to: ${logsPath}`);

        // List contents (tar auto-detects compression format with -tf)
        try {
          console.log('\nLogs archive contents (pytest logs):');
          const listing = execFileSync('tar', ['-tf', logsPath]).toString();
          const contents = listing.split('\n').filter(l => /pytest.*\.(log|jsonl)$/.test(l)).slice(0, 20).join('\n');
          console.log(contents || '(no pytest logs found)');
        } catch (e) {
          // Ignore errors from grep/head
        }
      } else {
        console.log('\nNo logs.tar.gz found in artifacts');
      }
    }

    return { testResults: filteredResults, artifactLinks, jsonData };

  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === '--help') {
    console.log(`
Usage: node fetch_ci_report.js <url> [options]

URL formats:
  - GitHub PR: https://github.com/ClickHouse/ClickHouse/pull/12345 (fetches ALL CI reports)
  - CI HTML:   https://s3.amazonaws.com/.../praktika.html?PR=...&sha=...&name_0=...
  - Direct JSON: https://s3.amazonaws.com/.../result_*.json

Options:
  --test <name>    Filter to show only tests matching this name
  --failed         Show failed test names in PR summary
  --all            Show all test results (not just summary)
  --links          Show artifact links
  --binary         Print the clickhouse binary URL to stdout (only); suitable for shell capture
  --cidb           Show CIDB links for failed tests
  --download-logs [path]  Download logs to path (default: /tmp/ci_logs.tar.{gz,zst})
  --report <number> For PR URLs: fetch only one specific report (default: fetch all)
  --credentials <user,password>  HTTP Basic Auth credentials

Examples:
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171"
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --failed --cidb
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --report 2
  node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?PR=94537&sha=abc123&name_0=PR&name_1=Integration%20tests"
  node fetch_ci_report.js "<url>" --test peak_memory --links
  node fetch_ci_report.js "<url>" --binary
  node fetch_ci_report.js "<url>" --failed --download-logs
`);
    process.exit(0);
  }

  const url = args[0];
  const options = {
    testFilter: null,
    failedOnly: false,
    showAll: false,
    showLinks: false,
    binary: false,
    showCidb: false,
    downloadLogs: false,
    reportIndex: null,
    credentials: null,
  };

  for (let i = 1; i < args.length; i++) {
    switch (args[i]) {
      case '--test':
        options.testFilter = args[++i];
        break;
      case '--failed':
        options.failedOnly = true;
        break;
      case '--all':
        options.showAll = true;
        break;
      case '--links':
        options.showLinks = true;
        break;
      case '--binary':
        options.binary = true;
        break;
      case '--cidb':
        options.showCidb = true;
        break;
      case '--download-logs':
        // Optional path argument: if next arg doesn't start with -- and isn't a URL, use it as path
        if (i + 1 < args.length && !args[i + 1].startsWith('--') && !args[i + 1].startsWith('http')) {
          options.downloadLogs = args[++i];
        } else {
          options.downloadLogs = true;
        }
        break;
      case '--report':
        options.reportIndex = args[++i];
        break;
      case '--credentials': {
        const cred = args[++i];
        const commaIdx = cred.indexOf(',');
        if (commaIdx === -1) {
          console.error('Error: --credentials must be in "user,password" format');
          process.exit(1);
        }
        options.credentials = {
          username: cred.substring(0, commaIdx),
          password: cred.substring(commaIdx + 1),
        };
        break;
      }
    }
  }

  // When --binary is set, all diagnostic output goes to stderr so stdout is clean for capture.
  if (options.binary) {
    console.log = (...args) => process.stderr.write(args.map(String).join(' ') + '\n');
  }

  await fetchReport(url, options);
}

if (require.main === module) {
  main().catch(console.error);
} else {
  module.exports = { parseTestResults, isFailureStatus, applyExtToTest };
}
