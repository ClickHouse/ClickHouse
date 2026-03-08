Apply the following TSan fix to the ClickHouse codebase.

## Fix Details
Read the RCA report from: {{RCA_FILE}}
(Use the Read tool to load the file contents before proceeding.)

## Previous Progress
Read the progress file from: {{PROGRESS_FILE}}
(Use the Read tool. If the file does not exist, this is the first iteration — no previous context.)

## Threading Model
Read the threading model from: {{THREADING_MODEL_FILE}}
(Use the Read tool.)

## Changes Already Applied
Run `git diff` to see uncommitted changes. If the output is empty, no changes have been applied yet.

## Instructions
1. Read the target source files — use the CURRENT file contents, not cached versions. Do NOT commit changes.
2. Apply the proposed code change using Edit tool
3. If adding TSA annotations, use macros from `base/base/defines.h`
4. If adding includes, place in alphabetical order within their group
5. Follow ClickHouse code style:
   - Allman-style braces (opening brace on a new line)
   - Never use sleep to fix race conditions
   - Prefer `std::lock_guard` or `std::unique_lock` for RAII locking
   - Use TSA annotations when adding mutex protection
6. If your fix changes threading invariants (e.g., a method no longer holds a lock when calling another method), update `{{THREADING_MODEL_FILE}}` directly (using Edit tool) — follow the format described in that file's header
7. Report what was changed
