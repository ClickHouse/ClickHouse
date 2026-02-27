---
name: build
description: Build ClickHouse with various configurations (Release, Debug, ASAN, TSAN, etc.). Use when the user wants to compile ClickHouse.
argument-hint: "[build-type] [target] [options]"
disable-model-invocation: false
allowed-tools: Task, Bash(ninja:*), Bash(cd:*), Bash(ls:*), Bash(pgrep:*), Bash(ps:*), Bash(pkill:*), Bash(mktemp:*), Bash(sleep:*)
---

# ClickHouse Build Skill

Build ClickHouse in `build` or `build_debug`, `build_asan`, `build_tsan`, `build_msan`, `build_ubsan` or, if exists, `build/${buildType}` directory.

## Arguments

- `$0` (optional): Build type - one of: `Release`, `Debug`, `RelWithDebInfo`, `ASAN`, `TSAN`, `MSAN`, `UBSAN`. Default: `RelWithDebInfo`
- `$1` (optional): Target - specific target to build (e.g., `clickhouse-server`, `clickhouse-client`, `clickhouse`). Default: `clickhouse`
- `$2+` (optional): Additional cmake/ninja options

## Build Types

| Type | Description | CMake Flags |
|------|-------------|-------------|
| `Release` | Optimized production build | `-DCMAKE_BUILD_TYPE=Release` |
| `Debug` | Debug build with symbols | `-DCMAKE_BUILD_TYPE=Debug` |
| `RelWithDebInfo` | Optimized with debug info | `-DCMAKE_BUILD_TYPE=RelWithDebInfo` |
| `ASAN` | AddressSanitizer (memory errors) | `-DCMAKE_BUILD_TYPE=Debug -DSANITIZE=address` |
| `TSAN` | ThreadSanitizer (data races) | `-DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread` |
| `MSAN` | MemorySanitizer (uninitialized reads) | `-DCMAKE_BUILD_TYPE=Debug -DSANITIZE=memory` |
| `UBSAN` | UndefinedBehaviorSanitizer | `-DCMAKE_BUILD_TYPE=Debug -DSANITIZE=undefined` |

## Common Targets

- `clickhouse` - Main all-in-one binary
- `clickhouse-server` - Server component
- `clickhouse-client` - Client component
- `clickhouse-local` - Local query processor
- `clickhouse-benchmark` - Benchmarking tool

## Build Process

**IMPORTANT:** This skill assumes the build directory is already configured with CMake. Do NOT run `cmake` or create build directories - only run `ninja`.

1. **Determine build configuration:**
   - Build type: `$0` or default (auto-detected) if not specified
   - Target: `$1` or `clickhouse` if not specified. **Always default to building `clickhouse`** — only build a different target if the user explicitly asks for one.
   - Build directory: auto-detected (see step 1a)

   **Step 1a: Auto-detect build directory:**

   Search for an existing, configured build directory (one containing `build.ninja`) in the following order. Stop at the first match:

   1. If a specific build type was requested (e.g., `Debug`, `ASAN`):
      - `build_<lowercase_type>/` (e.g., `build_debug/`, `build_asan/`)
      - `build/<type>/` (e.g., `build/Debug/`, `build/ASAN/`)
   2. For any build type (including default when none specified):
      - `build/` (check for `build.ninja` directly in `build/`)
      - `build/RelWithDebInfo/`
      - `build/Debug/`
      - `build_debug/`
      - `build_asan/`
      - `build_tsan/`
      - `build_msan/`
      - `build_ubsan/`

   **Verification:** The candidate directory must contain a `build.ninja` file to be considered valid.

   **If no configured build directory is found:**
   - Use `AskUserQuestion` to ask the user:
     - Question: "No configured build directory found. Where is your build directory?"
     - Option 1: "Let me configure it" - Description: "I'll run cmake to set up a build directory first"
     - Option 2: "Specify path" - Description: "Enter the path to an existing build directory"
   - Do NOT proceed without a valid build directory.

   **If a build directory is found:**
   - Report to the user: "Using build directory: `<path>`"
   - If no specific build type was requested, infer it from the directory name (e.g., `build/Debug` → Debug, `build_asan` → ASan).

2. **Create log file and start the build:**

   **Step 2a: Create temporary log file first:**
   ```bash
   mktemp /tmp/build_clickhouse_XXXXXX.log
   ```
   - This will print the log file path
   - **IMMEDIATELY report to the user:**
     - "Build logs will be written to: [log file path]"
     - Then display in a copyable code block:
       ```bash
       tail -f [log file path]
       ```
     - Example: "You can monitor the build in real-time with:" followed by the tail command in a code block

   **Step 2b: Start the ninja build:**
   ```bash
   cd [build_directory] && ninja [target] > [log file path] 2>&1
   ```
   Where `[build_directory]` is the path found in step 1a.

    When using ninja you can pass `-k{num}` to continue building even if some targets fail. For example, `-k20` will keep going after 20 failures. Adjust this number based on your needs.

   **Important:**
   - Do NOT create build directories or run `cmake` configuration
   - The build directory must already exist and be configured
   - Use the log file path from step 2a
   - Redirect both stdout and stderr to the log file using `> "$logfile" 2>&1`
   - Run the build in the background using `run_in_background: true`
   - **After starting the build**, report: "Build started in the background. Waiting for completion..."

3. **Wait for build completion:**
   - Use TaskOutput with `block=true` to wait for the background task to finish
   - The log file path is already known from step 2a
   - Pass the log file path to the Task agent in step 4

4. **Report results:**

   **ALWAYS use Task tool to analyze results** (both success and failure):
   - Use Task tool with `subagent_type=general-purpose` to analyze the build output
   - **Pass the log file path from step 2a** to the Task agent - let it read the file directly
   - Example Task prompt: "Read and analyze the build output from: /tmp/build_clickhouse_abc123.log"
   - The Task agent should read the file and provide:

     **If build succeeds:**
     - Confirm build completed successfully
     - Report binary location: `[build_directory]/programs/[target]`
     - Mention any warnings if present
     - Report build time if available
     - Keep response concise

     **If build fails:**
     - Parse the build output to identify what failed (compilation, linking, etc.)
     - Extract and highlight the specific error messages with file paths and line numbers
     - Identify compilation errors with the exact error text
     - For linker errors, identify missing symbols or libraries
     - For template errors, simplify and extract the core issue from verbose C++ template error messages
     - Provide the root cause if identifiable
     - Provide a concise summary with:
       - What component/file failed to build
       - The specific error type (syntax error, undefined symbol, etc.)
       - File path and line number where error occurred
       - The actual error message
       - Brief explanation of likely cause if identifiable
     - Filter out excessive verbose compiler output and focus on the actual errors

   - Return ONLY the Task agent's summary to the user
   - Do NOT return the full raw build output

   **After receiving the summary:**
   - If build succeeded: Proceed to step 5 to check for running server
   - If build failed:
     - Present the summary to the user first
     - **MANDATORY:** Use `AskUserQuestion` to prompt: "Do you want deeper analysis of this build failure?"
       - Option 1: "Yes, investigate further" - Description: "Launch a subagent to investigate the root cause across the codebase"
       - Option 2: "No, I'll fix it myself" - Description: "Skip deeper analysis and proceed without investigation"
     - If user chooses "Yes, investigate further":
       - **CRITICAL: DO NOT read files, edit code, or fix the issue yourself**
       - **MANDATORY: Use Task tool to launch a subagent for deep analysis only (NO FIXES)**
       - Use Task tool with `subagent_type=Explore` to search for related code patterns, similar errors, or find where symbols/functions are defined
       - For complex errors involving multiple files or dependencies, use Task tool with `subagent_type=general-purpose` to investigate missing symbols, headers, or dependencies
       - Provide specific prompts to the agent based on the error type:
         - Compilation errors: "Analyze the compilation error in [file:line]. Find where [symbol/class/function] is defined in the codebase and explain the root cause. Do NOT fix the code."
         - Linker errors: "Investigate why [symbol] is undefined and find its implementation. Explain what's causing the linker error. Do NOT fix the code."
         - Header errors: "Find which header file provides [missing declaration] and explain what's missing. Do NOT fix the code."
         - Template errors: "Investigate the template instantiation issue with [template name] and explain the root cause. Do NOT fix the code."
       - The subagent should only investigate and analyze, NOT edit or fix code
       - **CRITICAL: Return ONLY the agent's summary of findings to the user**
       - **DO NOT return full investigation details, raw file contents, or excessive verbose output**
       - **Present findings in a well-organized summary format**
     - If user chooses "No, I'll fix it myself":
       - Skip deeper analysis
     - Skip step 5 (no need to check for running server if build failed)

5. **MANDATORY: Check for running server and offer to stop it (only after successful build):**

   **IMPORTANT:** This step MUST be performed after every successful build. Do not skip this step.

   **Use Task tool with `subagent_type=general-purpose` to handle server checking and stopping:**

   ```
   Task tool with subagent_type=general-purpose
   Prompt: "Check if a ClickHouse server is currently running and handle it.

   Steps:
   1. Check for running ClickHouse server:
      pgrep -f \"clickhouse[- ]server\" | xargs -I {} ps -p {} -o pid,cmd --no-headers 2>/dev/null | grep -v \"cmake|ninja|Building\"

   2. If a server is running:
      - Report the PID and explain it's using the old binary
      - Use AskUserQuestion to ask: \"A ClickHouse server is currently running. Do you want to stop it so the new build can be used?\"
        - Option 1: \"Yes, stop the server\" - Description: \"Stop the running server (you'll need to start it manually later)\"
        - Option 2: \"No, keep it running\" - Description: \"Keep the old server running (won't use the new build)\"
      - If user chooses \"Yes, stop the server\":
        - Run: pkill -f \"clickhouse[- ]server\"
        - Wait 1 second: sleep 1
        - Verify stopped: pgrep -f \"clickhouse[- ]server\" should return nothing
        - Report: \"Server stopped. To start the new version, run: ./[build_directory]/programs/clickhouse server --config-file ./programs/server/config.xml\"
      - If user chooses \"No, keep it running\":
        - Report: \"Server remains running with the old binary. You'll need to manually restart it to use the new build.\"

   3. If no server is running:
      - Report: \"No ClickHouse server is currently running.\"

   Keep the response concise and only report the outcome to the user."
   ```

   - Wait for the Task agent to complete
   - Return the Task agent's summary to the user

6. **MANDATORY: Provide final summary to user:**

   After completing all steps, always provide a concise final summary to the user:

   **For successful builds:**
   - Confirm the build completed successfully
   - Report the binary location: `[build_directory]/programs/[target]`
   - Report the server status outcome from step 5

   **For failed builds:**
   - Already handled in step 4 with error analysis and optional investigation

   **Example final summary for successful build:**
   ```
   Build completed successfully!

   Binary: [build_directory]/programs/clickhouse
   Server status: No ClickHouse server is currently running.
   ```

   Keep the summary brief and clear.

## Examples

- `/build` - Build `clickhouse` target in RelWithDebInfo mode (default)
- `/build Debug clickhouse-server` - Build server in Debug mode
- `/build ASAN` - Build with AddressSanitizer
- `/build Release clickhouse-client` - Build only the client in Release mode

## Notes

- Always run from repository root
- **NEVER** create build directories or run `cmake` - the build directory must already be configured
- Build directories follow patterns: `build/`, `build/<type>/`, `build_<type>/` (e.g., `build_debug/`, `build/RelWithDebInfo/`)
- Binaries are located in: `[build_directory]/programs/`
- This skill only runs incremental builds with `ninja`
- To configure a new build directory, the user must manually run CMake first
- For a clean build, the user should remove `build` and reconfigure manually
- **MANDATORY:** After successful builds, this skill MUST check for running ClickHouse servers and ask the user if they want to stop them to use the new build
- **MANDATORY:** ALL build output (success or failure) MUST be analyzed by a Task agent with `subagent_type=general-purpose`
- **MANDATORY:** ALWAYS provide a final summary to the user at the end of the skill execution (step 6)
- **CRITICAL:** Build output is redirected to a unique log file created with `mktemp`. The log file path is reported to the user in a copyable format BEFORE starting the build, allowing real-time monitoring with `tail -f`. The log file path is saved from step 2a and passed to the Task agent for analysis. This keeps large build logs out of the main context.
- **Subagents available:** Task tool is used to analyze all build output (by reading from output file) and provide concise summaries. Additional agents (Explore or general-purpose) can be used for deeper investigation of complex build errors
