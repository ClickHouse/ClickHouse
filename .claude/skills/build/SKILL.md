---
name: build
description: Build ClickHouse with various configurations (Release, Debug, ASAN, TSAN, etc.). Use when the user wants to compile ClickHouse.
argument-hint: [build-type] [target] [options]
disable-model-invocation: false
allowed-tools: Task, Bash(ninja:*), Bash(cd:*), Bash(ls:*), Bash(pgrep:*), Bash(ps:*), Bash(pkill:*)
---

# ClickHouse Build Skill

Build ClickHouse in `build/${buildType}` directory (e.g., `build/Debug`, `build/ASAN`, `build/RelWithDebInfo`).

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
   - Build type: `$0` or `RelWithDebInfo` if not specified
   - Target: `$1` or `clickhouse` if not specified
   - Build directory: `build/${buildType}` (e.g., `build/RelWithDebInfo`, `build/Debug`, `build/ASAN`)

2. **Build the target directly with ninja:**
   ```bash
   cd build/${buildType}

   # Use ninja (preferred) - ninja auto-detects parallelism
   ninja [target]
   ```

   **Important:**
   - Do NOT create build directories or run `cmake` configuration
   - The build directory must already exist and be configured
   - Run the build in the background using `run_in_background: true`
   - **IMMEDIATELY after starting the build**, report the output file location and provide a copiable command:
     ```
     Build output is being written to: /tmp/claude/.../tasks/[task_id].output

     Monitor build progress:
     tail -f /tmp/claude/.../tasks/[task_id].output
     ```
   - Then wait for the build to complete using TaskOutput

3. **Report results:**

   **ALWAYS use Task tool to analyze results** (both success and failure):
   - Use Task tool with `subagent_type=general-purpose` to analyze the build output
   - The Task agent should analyze the full output and provide:

     **If build succeeds:**
     - Confirm build completed successfully
     - Report binary location: `build/${buildType}/programs/[target]`
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
   - If build succeeded: Proceed to step 4 to check for running server
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
       - Return ONLY the agent's analysis summary to the user
     - If user chooses "No, I'll fix it myself":
       - Skip deeper analysis
     - Skip step 4 (no need to check for running server if build failed)

4. **MANDATORY: Check for running server and offer to stop it (only after successful build):**
   ```bash
   pgrep -f "clickhouse[- ]server" | xargs -I {} ps -p {} -o pid,cmd --no-headers 2>/dev/null | grep -v "cmake\|ninja\|Building"
   ```

   **IMPORTANT:** This step MUST be performed after every successful build. Do not skip this step.

   - If a ClickHouse server is running:
     - Report that a server is currently running with its PID
     - Explain that the server is using the old binary and needs to be restarted to use the newly built version
     - **MANDATORY:** Use `AskUserQuestion` to prompt: "A ClickHouse server is currently running. Do you want to stop it so the new build can be used?"
       - Option 1: "Yes, stop the server" - Description: "Stop the running server (you'll need to start it manually later)"
       - Option 2: "No, keep it running" - Description: "Keep the old server running (won't use the new build)"
     - If user chooses "Yes, stop the server":
       ```bash
       pkill -f "clickhouse[- ]server"
       ```
       - Wait 1 second and verify the process was killed successfully
       - Confirm to user: "Server stopped. To start the new version, run: `./build/${buildType}/programs/clickhouse server --config-file ./programs/server/config.xml`"
     - If user chooses "No, keep it running":
       - Inform user: "Server remains running with the old binary. You'll need to manually restart it to use the new build."

   - If no server is running:
     - No action needed, proceed normally

## Examples

- `/build` - Build `clickhouse` target in RelWithDebInfo mode (default)
- `/build Debug clickhouse-server` - Build server in Debug mode
- `/build ASAN` - Build with AddressSanitizer
- `/build Release clickhouse-client` - Build only the client in Release mode

## Notes

- Always run from repository root
- **NEVER** create build directories or run `cmake` - the build directory must already be configured
- Build directories follow pattern: `build/${buildType}` (e.g., `build/Debug`, `build/ASAN`)
- Binaries are located in: `build/${buildType}/programs/`
- This skill only runs incremental builds with `ninja`
- To configure a new build directory, the user must manually run CMake first
- For a clean build, the user should remove `build/${buildType}` and reconfigure manually
- **MANDATORY:** After successful builds, this skill MUST check for running ClickHouse servers and ask the user if they want to stop them to use the new build
- **MANDATORY:** ALL build output (success or failure) MUST be analyzed by a Task agent with `subagent_type=general-purpose`
- **Subagents available:** Task tool is used to analyze all build output and provide concise summaries. Additional agents (Explore or general-purpose) can be used for deeper investigation of complex build errors
