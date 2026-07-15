# Downstream (Sync) CI Reports

## Overview

When a PR is opened on the public repository, a sync PR may be created in a downstream
repository. The downstream CI runs additional tests against the same code.

The `json.html` report page can display these downstream results alongside the upstream
results in a single merged table — for team members connected to a VPN with a configured
proxy.

## How It Works

1. When a downstream sync PR starts, it uploads metadata to S3:
   `<private-bucket>/sync_metadata/<upstream_sha>.json`
   ```json
   {
       "private_pr_number": 55000,
       "private_sha": "abc123...",
       "upstream_pr_number": 102000
   }
   ```

2. The public `json.html` page (at top level):
   - Renders public results immediately
   - If a proxy URL is configured, fetches the sync metadata via the proxy
   - If metadata exists, fetches the downstream `result_pr.json`
   - Merges downstream results into the table with a `Downstream:` name prefix
   - Shows error messages if any step fails

3. Clicking a downstream result name opens the downstream `json.html` served from the
   proxy (in a new tab), where further drill-down works automatically.

## Setup

### Prerequisites

- VPN connection (e.g. Tailscale)
- An HTTPS reverse proxy with CORS headers that provides access to the private S3 bucket

### Configure the Proxy URL

**Option A: URL parameter** (one-time, gets persisted to localStorage):
```
https://s3.amazonaws.com/<bucket>/json.html?PR=123&sha=abc&name_0=PR&proxy=https://<proxy-host>/<private-bucket>
```

**Option B: Footer toggle** — click the link icon in the page footer and enter the
proxy base URL.

**Option C: Browser console**:
```javascript
localStorage.setItem('privateBaseUrl', 'https://<proxy-host>/<private-bucket>');
location.reload();
```

To disable, clear the value via the footer toggle (enter empty string) or:
```javascript
localStorage.removeItem('privateBaseUrl');
```

## Error Messages

| Message | Meaning |
|---------|---------|
| `Downstream: proxy unreachable` | Proxy URL is configured but the fetch failed (not on VPN, proxy down, or CORS issue) |
| `Downstream: no sync metadata for this commit` | No downstream sync PR exists for this upstream commit SHA |
| `Downstream: sync PR results not available yet` | Metadata exists but the downstream CI has not uploaded results yet |
| `Downstream: invalid sync metadata` | Metadata JSON is malformed |

## Architecture

```
Browser (json.html on S3)
    |
    |-- fetch result_pr.json from S3 (public, immediate)
    |-- render public results
    |
    |-- fetch sync_metadata/<sha>.json via HTTPS proxy (async)
    |-- fetch downstream result_pr.json via HTTPS proxy
    |-- merge + re-render table with "Downstream:" prefix
```

The proxy must serve HTTPS and return CORS headers
(`Access-Control-Allow-Origin: https://s3.amazonaws.com`) so the S3-hosted page can
fetch from it.

Links inside downstream results (logs, reports) are rewritten from the original
domain to the proxy URL so they remain accessible.
