---
name: test
description: Run ClickHouse stateless or integration tests. Use when the user wants to run or execute tests.
argument-hint: [test-name] [--flags]
disable-model-invocation: false
allowed-tools: Task, Bash(./tests/clickhouse-test:*), Bash(pgrep:*), Bash(./build/*/programs/clickhouse:*), Bash(python:*), Bash(python3:*), Bash(mktemp:*), Bash(export:*)
---

# ClickHouse Test Runner Skill

Run stateless tests from `tests/queries/0_stateless/` or integration tests from `tests/integration/`.

## Arguments

- `$0` (optional): Test name (e.g., `03312_issue_63093` for stateless or `test_keeper_three_nodes_start` for integration), or empty to prompt for selection
- `$1+` (optional): Additional flags for test runner (e.g., `--no-random-settings`, `--record` for stateless tests)

## Test Types

The skill automatically detects the test type:
- **Stateless tests**: Located in `tests/queries/0_stateless/`, named like `NNNNN_description` (e.g., `03312_issue_63093`)
- **Integration tests**: Located in `tests/integration/`, named like `test_*` (e.g., `test_keeper_three_nodes_start`)

Detection logic:
1. If test name starts with `test_` → Integration test
2. If test name matches pattern `\d{5}_.*` → Stateless test
3. If currently viewing file in `tests/integration/test_*/` → Integration test
4. If currently viewing file in `tests/queries/0_stateless/` → Stateless test

## Test Selection

If no test name is provided in arguments, prompt the user with `AskUserQuestion`:

**Question: "Which test would you like to run?"**
- **Option 1: "Currently viewed test"** - Extract test name from currently opened file in IDE
  - Description: "Run the test file currently open in your editor"
  - Only available if a test file is currently open in the IDE
  - For stateless: Extract filename without extension from `tests/queries/0_stateless/03312_issue_63093.sh` → `03312_issue_63093`
  - For integration: Extract directory name from `tests/integration/test_keeper_three_nodes_start/test.py` → `test_keeper_three_nodes_start`

- **Option 2: "Custom test name"** - User provides test name
  - Description: "Specify a test name manually"
  - User can provide the test name via the "Other" field
  - Stateless examples: `03312_issue_63093`, `00029_test_zookeeper`
  - Integration examples: `test_keeper_three_nodes_start`, `test_access_control_on_cluster`

## Test Execution Process

### For Stateless Tests

**IMPORTANT**: ALWAYS perform the server check at the start of EVERY stateless test execution, even for repeated runs.

1. **Check if ClickHouse server is running (MANDATORY for stateless tests):**
   - **CRITICAL**: Use Task tool with `subagent_type=general-purpose` to perform server liveness check
   - The Task agent should:
     - Check if any `clickhouse-server` process is running (filter out build processes like cmake/ninja)
     - Test if server responds to a simple query: `SELECT 1`
     - Report status: "Server is running and healthy" or "Server is not running" or "Server is running but not responding"

   Example Task prompt:
   ```
   Check if the ClickHouse server is running and responding to queries.

   Perform these checks:
   1. Check if any clickhouse-server process is running (filter out build processes like cmake/ninja)
      Command: pgrep -f "clickhouse[- ]server" | xargs -I {} ps -p {} -o pid,cmd --no-headers 2>/dev/null | grep -v "cmake\|ninja\|Building"

   2. Test if the server responds to a simple query: SELECT 1
      Command: ./build/RelWithDebInfo/programs/clickhouse client -q "SELECT 1" 2>/dev/null

   Report:
   - Whether a server process is running (show PID and command if found)
   - Whether the server responds to queries
   - Overall status: "Server is running and healthy" or "Server is not running" or "Server is running but not responding"
   ```

   - **If server is not running or not responding:**
     - Report the Task agent's finding
     - Provide instructions: "Start the server with: `./build/RelWithDebInfo/programs/clickhouse server --config-file ./programs/server/config.xml`"
     - Use `AskUserQuestion` to prompt: "Did you start the ClickHouse server?"
       - Option 1: "Yes, server is running now" - Run the liveness check Task again to verify
       - Option 2: "No, I'll start it later" - Exit without running the test
     - If user confirms server is running, run the liveness check Task again before proceeding

   - **If server is running and healthy:**
     - Proceed to run the test

   - Note: Build directory path is configured via `/install-skills` (currently: `build/RelWithDebInfo`)

2. **Determine test name and type:**
   - If `$ARGUMENTS` is provided, use it as the test name
   - Otherwise, use `AskUserQuestion` to prompt user for test selection
   - Detect test type using patterns described in "Test Types" section
   - For stateless: Test name should NOT include file extension (`.sql`, `.sh`, etc.)

3. **Create log file and run the stateless test:**

   **Step 3a: Create temporary log file first:**
   ```bash
   mktemp /tmp/test_clickhouse_XXXXXX.log
   ```
   - This will print the log file path
   - **IMMEDIATELY report to the user:**
     - "Test logs will be written to: [log file path]"
     - Then display in a copyable code block:
       ```bash
       tail -f [log file path]
       ```
     - Example: "You can monitor the test progress in real-time with:" followed by the tail command in a code block

   **Step 3b: Start the stateless test:**
   ```bash
   # Add clickhouse binary to PATH
   # Configured via /install-skills (currently: build/RelWithDebInfo)
   export PATH="./build/RelWithDebInfo/programs:$PATH" && ./tests/clickhouse-test <test_name> [flags] > [log file path] 2>&1
   ```

   **Important:**
   - Run from repository root directory
   - Use the log file path from step 3a
   - Redirect both stdout and stderr to the log file using `> "$logfile" 2>&1`
   - Run in the background using `run_in_background: true`
   - **After starting the test**, report: "Test started in the background. Waiting for completion..."

   Common flags to mention if user asks:
   - `--no-random-settings` - Disable settings randomization
   - `--no-random-merge-tree-settings` - Disable MergeTree settings randomization
   - `--record` - Update `.reference` files when output differs

4. **Wait for stateless test completion:**
   - Use TaskOutput with `block=true` to wait for the background task to finish
   - The log file path is already known from step 3a
   - Pass the log file path to the Task agent in step 5

### For Integration Tests

**Note**: Integration tests manage their own Docker containers, so no server check is needed.

1. **Determine test name:**
   - If `$ARGUMENTS` is provided, use it as the test name
   - Otherwise, use `AskUserQuestion` to prompt user for test selection
   - Test name should be the directory name (e.g., `test_keeper_three_nodes_start`)

2. **Create log file and run the integration test:**

   **Step 2a: Create temporary log file first:**
   ```bash
   mktemp /tmp/test_clickhouse_XXXXXX.log
   ```
   - This will print the log file path
   - **IMMEDIATELY report to the user:**
     - "Test logs will be written to: [log file path]"
     - Then display in a copyable code block:
       ```bash
       tail -f [log file path]
       ```
     - Example: "You can monitor the test progress in real-time with:" followed by the tail command in a code block

   **Step 2b: Start the integration test with praktika:**
   ```bash
   python -u -m ci.praktika run "integration" --test <test_name> > [log file path] 2>&1
   ```

   **Important:**
   - Run from repository root directory
   - Use `python -u` flag to ensure unbuffered output (so logs stream in real-time)
   - Use the log file path from step 2a
   - Redirect both stdout and stderr to the log file using `> "$logfile" 2>&1`
   - Integration tests use Docker containers (managed automatically)
   - Tests may take longer than stateless tests (container startup time)
   - Run in the background using `run_in_background: true`
   - **After starting the test**, report: "Test started in the background. Waiting for completion..."

3. **Wait for integration test completion:**
   - Use TaskOutput with `block=true` to wait for the background task to finish
   - The log file path is already known from step 2a
   - Pass the log file path to the Task agent in step 4

5. **Report results:**

   **For Stateless Tests:**

   **ALWAYS use Task tool to analyze results** (both pass and fail):
   - Use Task tool with `subagent_type=general-purpose` to analyze the test output
   - **Pass the log file path from step 3a** to the Task agent - let it read the file directly
   - Example Task prompt: "Read and analyze the test output from: /tmp/test_clickhouse_abc123.log"
   - The Task agent should read the file and provide:

     **If tests passed:**
     - Confirm all tests passed
     - Report execution time from clickhouse-test output
     - Show summary (e.g., "1 tests passed. 0 tests skipped. 3.51 s elapsed")
     - Keep response brief

     **If tests failed:**
     - Parse the clickhouse-test output to identify which test failed
     - Extract the relevant error messages and differences
     - Identify the root cause if possible
     - Provide a concise summary with:
       - Test name that failed
       - What assertion or comparison failed
       - Expected vs actual output (show the diff)
       - Any error messages or exceptions
       - Brief explanation of the root cause
     - Filter out excessive verbose logs and focus on the actual failure

   - Return ONLY the Task agent's summary to the user
   - Do NOT return the full raw test output

   **After receiving the summary (for stateless tests):**
   - If tests passed: Done, no further action needed
   - If tests failed:
     - Present the summary to the user first
     - **MANDATORY:** Use `AskUserQuestion` to prompt: "Do you want deeper analysis of this test failure?"
       - Option 1: "Yes, investigate further" - Description: "Launch a subagent to investigate the root cause across the codebase"
       - Option 2: "No, I'll fix it myself" - Description: "Skip deeper analysis and proceed without investigation"
     - If user chooses "Yes, investigate further":
       - **CRITICAL: DO NOT read files, edit code, or fix the issue yourself**
       - **MANDATORY: Use Task tool to launch a subagent for deep analysis only (NO FIXES)**
       - Use Task tool with `subagent_type=Explore` to search for related code patterns, or find where functions/queries are implemented
       - For complex failures involving multiple components, use Task tool with `subagent_type=general-purpose` to investigate root causes
       - Provide specific prompts to the agent based on the failure type:
         - Query failures: "Investigate why the query [query] returns [actual] instead of [expected]. Find the implementation and explain the root cause. Do NOT fix the code."
         - Assertion failures: "Analyze why [assertion] failed in test [test_name]. Find the relevant code and explain what's happening. Do NOT fix the code."
         - Output differences: "Investigate the difference between expected and actual output in test [test_name]. Explain why the output changed. Do NOT fix the code."
         - Exception/error: "Investigate the error [error_message] in test [test_name]. Find where it originates and explain the cause. Do NOT fix the code."
       - The subagent should only investigate and analyze, NOT edit or fix code
       - **CRITICAL: Return ONLY the agent's summary of findings to the user**
       - **DO NOT return full investigation details, raw file contents, or excessive verbose output**
       - **Present findings in a well-organized summary format**
     - If user chooses "No, I'll fix it myself":
       - Skip deeper analysis

   **For Integration Tests:**

   **ALWAYS use Task tool to analyze results** (both pass and fail):
   - Use Task tool with `subagent_type=general-purpose` to analyze the test output
   - **Pass the log file path from step 2a** to the Task agent - let it read the file directly
   - Example Task prompt: "Read and analyze the test output from: /tmp/test_clickhouse_abc123.log"
   - The Task agent should read the file and provide:

     **If tests passed:**
     - Confirm all tests passed
     - Report total execution time from pytest output
     - Keep response brief: "All tests passed (XX.XXs)"

     **If tests failed:**
     - Parse the pytest output to identify which specific test cases failed
     - Extract the relevant error messages, assertions, and stack traces
     - Identify the root cause if possible (e.g., timeout, connection error, assertion failure)
     - Filter out verbose Docker logs and focus on the actual test failure
     - Provide a concise summary with:
       - Test name that failed
       - Line number where it failed
       - Specific assertion or error
       - Expected vs actual values
       - Brief explanation of the root cause

   - Return ONLY the Task agent's summary to the user
   - Do NOT return the full raw test output

   **After receiving the summary (for integration tests):**
   - If tests passed: Done, no further action needed
   - If tests failed:
     - Present the summary to the user first
     - **MANDATORY:** Use `AskUserQuestion` to prompt: "Do you want deeper analysis of this test failure?"
       - Option 1: "Yes, investigate further" - Description: "Launch a subagent to investigate the root cause across the codebase"
       - Option 2: "No, I'll fix it myself" - Description: "Skip deeper analysis and proceed without investigation"
     - If user chooses "Yes, investigate further":
       - **CRITICAL: DO NOT read files, edit code, or fix the issue yourself**
       - **MANDATORY: Use Task tool to launch a subagent for deep analysis only (NO FIXES)**
       - Use Task tool with `subagent_type=Explore` to search for related code patterns, or find where functions are implemented
       - For complex failures involving multiple components, use Task tool with `subagent_type=general-purpose` to investigate root causes
       - Provide specific prompts to the agent based on the failure type:
         - Timeout errors: "Investigate why test [test_name] times out. Find what operation is slow and explain the cause. Do NOT fix the code."
         - Connection errors: "Investigate the connection error [error] in test [test_name]. Find what's causing the connection issue. Do NOT fix the code."
         - Assertion failures: "Analyze the assertion failure at [file:line] in test [test_name]. Explain why the assertion failed. Do NOT fix the code."
         - Exception/error: "Investigate the exception [exception] in test [test_name]. Find where it originates and explain the cause. Do NOT fix the code."
       - The subagent should only investigate and analyze, NOT edit or fix code
       - **CRITICAL: Return ONLY the agent's summary of findings to the user**
       - **DO NOT return full investigation details, raw file contents, or excessive verbose output**
       - **Present findings in a well-organized summary format**
     - If user chooses "No, I'll fix it myself":
       - Skip deeper analysis

## Test File Structure

### Stateless Tests
- **Location**: `tests/queries/0_stateless/`
- **Extensions**: `.sql`, `.sh`, `.py`, `.sql.j2`, `.expect`
- **Reference files**: `.reference` (expected output)
- **Test name format**: `NNNNN_description` (e.g., `03312_issue_63093`)

### Integration Tests
- **Location**: `tests/integration/test_*/`
- **Format**: Python pytest files (`test.py` or `test_*.py`)
- **Directory structure**: Each test is a directory named `test_*`
- **Test name format**: `test_*` (e.g., `test_keeper_three_nodes_start`)
- **Dependencies**: Uses Docker containers, pytest fixtures, and helper modules

## Examples

### Stateless Tests
- `/test` - Prompt to select test (currently viewed or custom name)
- `/test 03312_issue_63093` - Run specific stateless test by name
- `/test 00029_test_zookeeper --no-random-settings` - Run stateless test with flags
- `/test 03312_issue_63093 --record` - Run stateless test and update reference files

### Integration Tests
- `/test test_keeper_three_nodes_start` - Run specific integration test
- `/test test_access_control_on_cluster` - Run integration test by name
- `/test` - If viewing `tests/integration/test_*/test.py`, automatically detect and run that integration test

## Environment Variables

The test runner automatically detects and sets the necessary environment variables for connecting to the server (for stateless tests).

## Notes

### General
- Run from repository root directory
- Test type is automatically detected based on name pattern or file location
- **MANDATORY:** ALL test output (success or failure) MUST be analyzed by a Task agent with `subagent_type=general-purpose`
- **MANDATORY:** For test failures, MUST prompt user if they want deeper analysis and use Task subagent if requested
- **CRITICAL:** Test output is redirected to a unique log file created with `mktemp`. The log file path is reported to the user in a copyable format BEFORE starting the test, allowing real-time monitoring with `tail -f`. The log file path is saved and passed to the Task agent for analysis. This keeps large test logs out of the main context.
- **Subagents available:** Task tool is used to analyze all test output (by reading from log file) and provide concise summaries. Additional agents (Explore or general-purpose) are used for deeper investigation of test failures when user requests it

### Stateless Tests
- Test names do NOT include extensions (use `03312_issue_63093`, not `03312_issue_63093.sh`)
- **CRITICAL**: Always check if server is running before attempting to run stateless tests - this check MUST be performed EVERY time, even for repeated test runs
- The server check protects against running tests when the server has crashed or been stopped
- Test runner creates temporary database with random name for isolation
- Reference files use `default` database name, not the random test database
- Build directory used: `build/RelWithDebInfo` (configured with `/install-skills`)
- Use `/install-skills test` to reconfigure which build directory to use for testing

### Integration Tests
- Test names are directory names (use `test_keeper_three_nodes_start`, not `test.py`)
- Integration tests manage their own Docker containers - no server check needed
- Tests may take longer due to container startup and teardown
- Integration tests use pytest framework with fixtures
- Docker daemon must be running and accessible
- Requires Python dependencies from `tests/integration/` directory
- Tests are run via praktika: `python -m ci.praktika run "integration" --test <test_name>`
