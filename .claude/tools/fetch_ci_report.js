#!/usr/bin/env node
/**
 * Fetch ClickHouse CI test reports without Playwright
 *
 * Usage:
 *   node fetch_ci_report.js <url> [options]
 *
 * URL formats supported:
 *   - GitHub PR URLs: https://github.com/ClickHouse/ClickHouse/pull/12345 (fetches ALL CI reports)
 *   - HTML URLs: https://s3.amazonaws.com/.../json.html?PR=...&sha=...&name_0=...
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
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&..."
 *   node fetch_ci_report.js "https://s3.amazonaws.com/.../result_integration_tests.json"
 *   node fetch_ci_report.js "<url>" --test peak_memory --links
 *   node fetch_ci_report.js "<url>" --failed --download-logs
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');
const fs = require('fs');
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
function fetchUrl(urlString, credentials = null) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(urlString);
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
        const body = Buffer.concat(chunks).toString('utf8');
        resolve(body);
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
  const url = new URL(htmlUrl);
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
    const commits = JSON.parse(commitsText);
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
function constructJsonUrl(baseUrl, suffix, sha, taskName) {
  const normalizedTask = normalizeTaskName(taskName);
  return `${baseUrl}/${suffix}/${encodeURIComponent(sha)}/result_${normalizedTask}.json`;
}

/**
 * Check if a status represents a failure
 */
function isFailureStatus(status) {
  return status === 'failed' || status === 'FAIL' || status === 'failure' ||
         status === 'error' || status === 'ERROR';
}

/**
 * Parse test results from the JSON data
 */
function parseTestResults(jsonData) {
  const tests = [];

  if (!jsonData || !jsonData.results) {
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

        // Include info field (contains build log tail for build failures)
        if (result.info) {
          test.info = result.info;
        }

        // Include links from this result
        if (result.links && result.links.length > 0) {
          test.links = result.links;
        }

        // Extract CIDB links and other labels, mirroring ci/praktika/json.html `normalizeLabels`:
        // `ext.labels` entries are either bare strings (legacy) or {name, link} objects, and
        // `ext.hlabels` entries are [name, link] pairs; all are merged by name (a link wins over a
        // bare occurrence). Non-cidb labels (e.g. `issue`, `retry_ok`) mirror how CI attributes a
        // failure: an `issue` label means CI matched a tracking issue; `retry_ok` etc. are the flags
        // an infrastructure issue matches on via `Failure flags:`.
        if (result.ext) {
          const byName = new Map();
          const upsert = (name, link) => {
            if (!name) return;
            const prev = byName.get(name) || {};
            byName.set(name, { name, link: link || prev.link });
          };
          if (Array.isArray(result.ext.labels)) {
            for (const item of result.ext.labels) {
              if (typeof item === 'string') {
                upsert(item);
              } else if (item && typeof item === 'object' && item.name) {
                upsert(item.name, item.link);
              }
            }
          }
          if (Array.isArray(result.ext.hlabels)) {
            for (const item of result.ext.hlabels) {
              if (Array.isArray(item) && item[0]) {
                upsert(item[0], item[1]);
              }
            }
          }
          const cidbLinks = [];
          const labels = [];
          for (const { name, link } of byName.values()) {
            if (name === 'cidb') {
              if (link) cidbLinks.push(link);
            } else {
              labels.push(link ? `${name} (${link})` : name);
            }
          }
          if (cidbLinks.length > 0) {
            test.cidbLinks = cidbLinks;
          }
          if (labels.length > 0) {
            test.labels = labels;
          }
        }

        tests.push(test);
      }
    }
  }

  extractTests(jsonData.results);
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

  // Filter to artifact/log links; exclude json.html navigation links and raw binaries
  return links.filter(link => {
    const h = link.href;
    // Exclude CI navigation/report links
    if (h.includes('json.html')) return false;
    // Include all log and archive formats
    if (h.includes('.log') || h.includes('.log.zst')) return true;
    if (h.includes('.tar.gz') || h.includes('.tar.zst') || h.includes('.tgz')) return true;
    if (h.includes('.zst')) return true;
    if (h.includes('.html') && !h.includes('json.html')) return true;
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
    const reportUrlPattern = /https:\/\/s3\.amazonaws\.com\/clickhouse-test-reports\/json\.html\?[^\s)`'"]+/g;
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
 * Given a top-level index report URL (json.html?...&name_0=X, no name_1) and its raw JSON, return
 * the per-job report URLs (json.html?...&name_1=<job>) for the FAILED jobs. Child names come from
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
 * Render a set of report URLs as a multi-report summary (one row per report, failures under each).
 * Shared by the GitHub-PR path and the direct top-level-index path.
 */
async function renderMultiReport(ciUrls, options) {
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
        console.log(`      ❌ FAIL: ${test.name}`);
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
          const lines = test.info.split('\n').filter(l => l.trim());
          const tail = lines.slice(-30);
          console.log('         --- log tail ---');
          for (const line of tail) {
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
      // job by synthesizing its per-job report URL (json.html?...&name_1=<job>, the same form the
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
          }
        } catch (e) {
          console.log(`Note: could not expand the top-level PR report into job reports (${e.message}); showing job-level failures only.`);
        }
      }

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

    // A direct top-level workflow result JSON (result_pr.json / result_masterci.json / result_ref.json
    // located directly under the <sha> dir — NOT under a job subdir) is a workflow index, just like
    // the json.html?...&name_0=PR form. Rewrite it to that HTML form so the index handling below
    // applies uniformly (expand into per-job reports; refuse per-job --download-logs) instead of
    // treating the whole PR/workflow as a single job. Concrete job reports are result_<job>.json
    // (or live under <sha>/<job>/...), so they never match this and stay on the single-report path.
    const topJson = inputUrl.match(/\/(?:PRs\/(\d+)|REFs\/([^/]+))\/([0-9a-f]{40})\/result_(?:pr|masterci|ref)\.json(?:$|\?)/i);
    if (topJson) {
      const prefix = inputUrl.slice(0, topJson.index);
      inputUrl = topJson[1]
        ? `${prefix}/json.html?PR=${topJson[1]}&sha=${topJson[3]}&name_0=PR`
        : `${prefix}/json.html?REF=${encodeURIComponent(topJson[2])}&sha=${topJson[3]}&name_0=MasterCI`;
    }

    // Check if this is a direct JSON URL or an HTML URL with parameters
    const isDirectJsonUrl = inputUrl.includes('.json') || !inputUrl.includes('?');

    if (isDirectJsonUrl) {
      // Direct JSON URL - fetch it directly
      if (!options.isSingleReport) {
        console.log(`Fetching JSON directly: ${inputUrl}\n`);
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
      const jsonUrl = constructJsonUrl(baseUrl, suffix, sha, nameParams[0]);
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
      if (isWorkflowIndex && nameParams.length === 1 && !options.isSingleReport && !options.reportIndex) {
        const topJson = JSON.parse(await fetchUrl(jsonUrl, options.credentials));
        const childUrls = childReportUrlsForFailedJobs(inputUrl, topJson);
        if (options.downloadLogs) {
          const failedNames = (topJson.results || []).filter(j => isFailureStatus(j.status)).map(j => j.name);
          throw new Error(
            `'${nameParams[0]}' is a top-level index report, not a single job — --download-logs would ` +
            `fetch the wrong job's artifacts. Re-run against a concrete job report by appending ` +
            `&name_1=<job>. Failed jobs: ${failedNames.join(', ') || '(none)'}.`
          );
        }
        console.log(`Top-level '${nameParams[0]}' index — expanding into ${childUrls.length} failed job report(s).\n`);
        return await renderMultiReport([inputUrl, ...childUrls], options);
      }

      // Fetch name_0 JSON data, and name_1 separately if present (matching json.html behavior)
      const fetchTasks = [fetchUrl(jsonUrl, options.credentials)];
      if (nameParams.length > 1) {
        const json1Url = constructJsonUrl(baseUrl, suffix, sha, nameParams[1]);
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

    // Print results for standalone report
    console.log('=== Test Results ===\n');

    const failed = filteredResults.filter(t => isFailureStatus(t.status));
    const passed = filteredResults.filter(t => t.status === 'success' || t.status === 'OK');
    const skipped = filteredResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

    console.log(`Total: ${filteredResults.length} | ✅ Passed: ${passed.length} | ❌ Failed: ${failed.length} | ⏭️  Skipped: ${skipped.length}\n`);

    if (failed.length > 0) {
      console.log('--- Failures ---');
      for (const test of failed) {
        console.log(`❌ FAIL  ${test.name}  (${test.duration}s)`);
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
          // Show last 30 non-empty lines of info (build log tail with actual errors)
          const lines = test.info.split('\n').filter(l => l.trim());
          const tail = lines.slice(-30);
          console.log('   --- log tail ---');
          for (const line of tail) {
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
  - CI HTML:   https://s3.amazonaws.com/.../json.html?PR=...&sha=...&name_0=...
  - Direct JSON: https://s3.amazonaws.com/.../result_*.json

Options:
  --test <name>    Filter to show only tests matching this name
  --failed         Show failed test names in PR summary
  --all            Show all test results (not just summary)
  --links          Show artifact links
  --cidb           Show CIDB links for failed tests
  --download-logs [path]  Download logs to path (default: /tmp/ci_logs.tar.{gz,zst})
  --report <number> For PR URLs: fetch only one specific report (default: fetch all)
  --credentials <user,password>  HTTP Basic Auth credentials

Examples:
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171"
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --failed --cidb
  node fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/97171" --report 2
  node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&sha=abc123&name_0=Integration%20tests"
  node fetch_ci_report.js "<url>" --test peak_memory --links
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

  await fetchReport(url, options);
}

main().catch(console.error);
