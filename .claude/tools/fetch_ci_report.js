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
 *   --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz
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
const { execSync } = require('child_process');
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
        // Leaf result - this is a test
        const test = {
          name: prefix ? `${prefix}/${result.name}` : result.name,
          status: result.status || 'UNKNOWN',
          duration: result.duration || 0
        };

        // Extract CIDB links from ext.hlabels
        if (result.ext && result.ext.hlabels) {
          const cidbLinks = [];
          for (const hlabel of result.ext.hlabels) {
            if (Array.isArray(hlabel) && hlabel[0] === 'cidb' && hlabel[1]) {
              cidbLinks.push(hlabel[1]);
            }
          }
          if (cidbLinks.length > 0) {
            test.cidbLinks = cidbLinks;
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

  // Filter to only artifact links
  return links.filter(link =>
    link.href.includes('.tar.gz') ||
    link.href.includes('.log') ||
    link.href.includes('configs')
  );
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

  // Fetch PR comments to find CI bot comment
  try {
    const commentsJson = execSync(`gh api repos/ClickHouse/ClickHouse/issues/${prNumber}/comments --jq '[.[] | select(.user.login == "clickhouse-gh[bot]") | {body, created_at}] | sort_by(.created_at) | reverse | .[0]'`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });

    const comment = JSON.parse(commentsJson);
    if (!comment || !comment.body) {
      throw new Error('No CI bot comment found');
    }

    // Extract CI report URLs from comment
    const reportUrlPattern = /https:\/\/s3\.amazonaws\.com\/clickhouse-test-reports\/json\.html\?[^\s)]+/g;
    const urls = comment.body.match(reportUrlPattern);

    if (!urls || urls.length === 0) {
      throw new Error('No CI report URLs found in bot comment');
    }

    return urls;
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
        // Fetch all reports
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

        for (const result of allResults) {
          if (result.error) {
            console.log(`[${result.index}] ${result.jobName}`);
            console.log(`    ❌ Error: ${result.error}\n`);
            continue;
          }

          const { testResults = [] } = result;
          const failed = testResults.filter(t => t.status === 'failed' || t.status === 'FAIL');
          const passed = testResults.filter(t => t.status === 'success' || t.status === 'OK');
          const skipped = testResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

          totalTests += testResults.length;
          totalPassed += passed.length;
          totalFailed += failed.length;
          totalSkipped += skipped.length;

          const status = failed.length > 0 ? '❌' : '✅';
          console.log(`[${result.index}] ${status} ${result.jobName}`);
          console.log(`    Total: ${testResults.length} | ✅ Passed: ${passed.length} | ❌ Failed: ${failed.length} | ⏭️  Skipped: ${skipped.length}`);

          // For nested reports with failures, show the HTML link
          if (!result.isPRLevel && failed.length > 0 && result.url) {
            console.log(`    🔗 Report: ${result.url}`);
          }

          // Skip showing individual test failures for PR-level reports to avoid duplication
          if (failed.length > 0 && options.failedOnly && !result.isPRLevel) {
            for (const test of failed) {
              console.log(`      ❌ FAIL: ${test.name}`);
              if (options.showCidb && test.cidbLinks && test.cidbLinks.length > 0) {
                for (const cidbLink of test.cidbLinks) {
                  console.log(`         📊 CIDB: ${cidbLink}`);
                }
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
        t.status === 'failed' || t.status === 'FAIL'
      );
    }

    // If this is a single report being fetched (part of multi-report fetch), just return data
    if (options.isSingleReport) {
      return { testResults, artifactLinks, jsonData };
    }

    // Print results for standalone report
    console.log('=== Test Results ===\n');

    const failed = filteredResults.filter(t => t.status === 'failed' || t.status === 'FAIL');
    const passed = filteredResults.filter(t => t.status === 'success' || t.status === 'OK');
    const skipped = filteredResults.filter(t => t.status === 'skipped' || t.status === 'SKIPPED');

    console.log(`Total: ${filteredResults.length} | ✅ Passed: ${passed.length} | ❌ Failed: ${failed.length} | ⏭️  Skipped: ${skipped.length}\n`);

    if (failed.length > 0) {
      console.log('--- Failed Tests ---');
      for (const test of failed) {
        console.log(`❌ FAIL  ${test.name}  (${test.duration}s)`);
        if (options.showCidb && test.cidbLinks && test.cidbLinks.length > 0) {
          for (const cidbLink of test.cidbLinks) {
            console.log(`   📊 CIDB: ${cidbLink}`);
          }
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
      const logsLink = artifactLinks.find(l => l.href.includes('logs.tar.gz'));
      if (logsLink) {
        console.log(`\nDownloading logs from: ${logsLink.href}`);
        const logsPath = '/tmp/ci_logs.tar.gz';
        execSync(`curl -sL "${logsLink.href}" -o ${logsPath}`);
        console.log(`Logs saved to: ${logsPath}`);

        // List contents
        try {
          console.log('\nLogs archive contents (pytest logs):');
          const contents = execSync(`tar -tzf ${logsPath} | grep -E "pytest.*\\.log$|pytest.*\\.jsonl$" | head -20`).toString();
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
  --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz
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
        options.downloadLogs = true;
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
