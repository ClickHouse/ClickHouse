# HTML Page Development

## Page development and testing with an AI agent

This guide explains how to preview and validate the CI HTML page (`ci/praktika/json.html`) locally with an AI agent and how to deploy changes for test or production.

### Prerequisites

1. Configure Selenium MCP Server for your AI agent
   
   Add the following configuration to your MCP settings:
   ```json
   {
     "selenium": {
       "command": "npx",
       "args": ["-y", "@angiejones/mcp-selenium"]
     }
   }
   ```

2. Prepare a commit SHA and parameters
   
   You'll need a commit SHA and a base reports URL. The page accepts query parameters, for example:
   
   - `REF`: branch or ref name (e.g., `master`)
   - `sha`: the commit SHA to test
   - `base_url`: URL-encoded base URL to CI reports (encoded `https://s3.amazonaws.com/clickhouse-test-reports` becomes `https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports`)
   - `name_0`: display name for the data source (e.g., `MasterCI`)

### Quick start with an AI agent

Ask your agent to start Chrome with CORS disabled and open `./ci/praktika/json.html` with parameters, for example:

```
Please start Chrome with disabled CORS and open ./ci/praktika/json.html file with params:
REF=master&sha=YOUR_COMMIT_SHA&base_url=https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports&name_0=MasterCI
```

Note: Replace `YOUR_COMMIT_SHA` with the specific commit SHA you need to test.

### Manual local preview (without an agent)

On macOS, you can launch Google Chrome with web security disabled to allow local file access with remote resources:

```bash
open -a "Google Chrome" --args \
  --disable-web-security \
  --disable-site-isolation-trials \
  --allow-file-access-from-files \
  --user-data-dir="/tmp/ch-dev" \
  "file:///ABSOLUTE_PATH_TO_REPO/ci/praktika/json.html?REF=master&sha=YOUR_COMMIT_SHA&base_url=https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports&name_0=MasterCI"
```

- Replace `ABSOLUTE_PATH_TO_REPO` with your local path.
- Only use these flags for local testing. Do not use them for normal browsing.

### Full example (agent prompt)

```
Please start Chrome with disabled CORS and open ./ci/praktika/json.html file with params:
REF=master&sha=59d18f406d9ef99bbf400184fd0ba47a84381bec&base_url=https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports&name_0=MasterCI
```

---

# Deploy the HTML page

Run the following from the repository root.

## Test location

- Will not affect production.
- Can be accessed by replacing `json.html` with `json_test.html` in any CI report URL.

```bash
python -m ci.praktika html --test
```

## Production

- Will be immediately applied for all CI reports.

```bash
python -m ci.praktika html
```

### Troubleshooting

- If data fails to load locally, make sure Chrome was launched with CORS disabled (see flags above).
- Double-check that `sha` points to a valid commit with existing CI results.
- Ensure `base_url` is URL-encoded.
- If the page still doesn't render, open DevTools (Console/Network) to check for errors and blocked requests.
