#!/usr/bin/env node
/**
 * Fetch ClickHouse CI test reports using Playwright
 *
 * Usage:
 *   node fetch_ci_report.js <url> [options]
 *
 * Options:
 *   --test <name>    Filter to show only tests matching this name
 *   --failed         Show only failed tests
 *   --links          Show artifact links
 *   --download-logs  Download and extract logs.tar.gz
 *   --credentials <user,password>  Only for ClickHouse_private repository: HTTP Basic Auth credentials (comma-separated)
 *
 * Examples:
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&..."
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&..." --test peak_memory
 *   node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&..." --failed
 *
 * Setup (one-time):
 *   cd /tmp && npm install playwright && npx playwright install chromium
 */

let chromium;
try {
  // Try loading from /tmp where it's typically installed
  chromium = require('/tmp/node_modules/playwright').chromium;
} catch (e) {
  try {
    // Fall back to local node_modules
    chromium = require('playwright').chromium;
  } catch (e2) {
    console.error('Error: Playwright not found. Install it first:');
    console.error('  cd /tmp && npm install playwright && npx playwright install chromium');
    process.exit(1);
  }
}
const https = require('https');
const fs = require('fs');
const { execSync } = require('child_process');
const path = require('path');

async function fetchReport(url, options = {}) {
  const browser = await chromium.launch();
  const contextOptions = {};
  if (options.credentials) {
    contextOptions.httpCredentials = options.credentials;
  }
  const context = await browser.newContext(contextOptions);
  const page = await context.newPage();

  console.log(`Fetching: ${url}\n`);

  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
    await page.waitForTimeout(3000);

    // Get page content
    const content = await page.evaluate(() => document.body.innerText);

    // Extract artifact links
    const links = await page.evaluate(() => {
      const anchors = document.querySelectorAll('a');
      return Array.from(anchors).map(a => ({ href: a.href, text: a.innerText }));
    });

    // Filter artifact links
    const artifactLinks = links.filter(link =>
      link.href.includes('.tar.gz') ||
      link.href.includes('.log') ||
      link.href.includes('configs')
    );

    // Parse test results
    const lines = content.split('\n');
    const testResults = [];
    let inResults = false;

    for (const line of lines) {
      if (line.includes('status\tname\tduration')) {
        inResults = true;
        continue;
      }
      if (inResults && line.trim()) {
        const parts = line.split('\t');
        if (parts.length >= 3) {
          const status = parts[0].trim();
          const name = parts[1].trim();
          const duration = parts[2].trim();
          if (['OK', 'FAIL', 'SKIPPED'].some(s => status.includes(s))) {
            testResults.push({ status, name, duration });
          }
        }
      }
    }

    // Apply filters
    let filteredResults = testResults;

    if (options.testFilter) {
      filteredResults = filteredResults.filter(t =>
        t.name.toLowerCase().includes(options.testFilter.toLowerCase())
      );
    }

    if (options.failedOnly) {
      filteredResults = filteredResults.filter(t => t.status.includes('FAIL'));
    }

    // Print results
    console.log('=== Test Results ===\n');

    const failed = filteredResults.filter(t => t.status.includes('FAIL'));
    const passed = filteredResults.filter(t => t.status.includes('OK'));
    const skipped = filteredResults.filter(t => t.status.includes('SKIPPED'));

    console.log(`Total: ${filteredResults.length} | Passed: ${passed.length} | Failed: ${failed.length} | Skipped: ${skipped.length}\n`);

    if (failed.length > 0) {
      console.log('--- Failed Tests ---');
      for (const test of failed) {
        console.log(`FAIL  ${test.name}  (${test.duration}s)`);
      }
      console.log('');
    }

    if (options.showAll) {
      console.log('--- All Tests ---');
      for (const test of filteredResults) {
        console.log(`${test.status.padEnd(8)} ${test.name}  (${test.duration}s)`);
      }
    }

    if (options.showLinks) {
      console.log('\n=== Artifact Links ===');
      for (const link of artifactLinks) {
        console.log(`${link.text}: ${link.href}`);
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
        console.log('\nLogs archive contents (pytest logs):');
        const contents = execSync(`tar -tzf ${logsPath} | grep -E "pytest.*\\.log$" | head -20`).toString();
        console.log(contents);
      }
    }

    return { testResults: filteredResults, artifactLinks, content };

  } finally {
    await browser.close();
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === '--help') {
    console.log(`
Usage: node fetch_ci_report.js <url> [options]

Options:
  --test <name>    Filter to show only tests matching this name
  --failed         Show only failed tests
  --all            Show all test results (not just summary)
  --links          Show artifact links
  --download-logs  Download and extract logs.tar.gz
  --credentials <user,password>  HTTP Basic Auth credentials

Examples:
  node fetch_ci_report.js "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=94537&sha=abc123&name_0=PR&name_1=Integration%20tests"
  node fetch_ci_report.js "<url>" --test peak_memory --links
  node fetch_ci_report.js "<url>" --failed --download-logs
  node fetch_ci_report.js "<url>" --credentials "user,password" --failed
`);
    process.exit(0);
  }

  const url = args[0];
  const options = {
    testFilter: null,
    failedOnly: false,
    showAll: false,
    showLinks: false,
    downloadLogs: false,
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
      case '--download-logs':
        options.downloadLogs = true;
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
