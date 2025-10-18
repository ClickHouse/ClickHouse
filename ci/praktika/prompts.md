# HTML Page Development

## Prerequisites

1. **Configure Selenium MCP Server for your AI agent**
   
   Add the following configuration to your MCP settings:
   ```json
   "selenium": {
     "command": "npx",
     "args": ["-y", "@angiejones/mcp-selenium"]
   }
   ```

2. **Open the HTML page with test parameters**
   
   Ask your agent to start Chrome with CORS policy disabled and open `./ci/praktika/json.html` with the following parameters:
   
   ```
   REF=master&sha=YOUR_COMMIT_SHA&base_url=https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports&name_0=MasterCI
   ```
   
   **Note:** Replace the `sha` parameter with the specific commit SHA you need to test.

## Example Usage

```
Please start Chrome with disabled CORS and open ./ci/praktika/json.html file with params:
REF=master&sha=59d18f406d9ef99bbf400184fd0ba47a84381bec&base_url=https%3A%2F%2Fs3.amazonaws.com%2Fclickhouse-test-reports&name_0=MasterCI
```
