---
name: install-skills
description: Configure and customize skills for your workspace by asking questions and updating skill files with your preferences.
argument-hint: [skill-name]
disable-model-invocation: false
allowed-tools: Read, Edit, AskUserQuestion
---

# Install Skills Configuration Tool

Configure skills by asking the user for their preferences and updating the skill files accordingly.

## Arguments

- `$0` (optional): Skill name to configure. If not provided, configure all available skills that need setup.

## Supported Skills

### Build Skill

The `build` skill can be configured with:

1. **Build directory pattern**: Where builds are stored relative to workspace
   - **Always prompt with at least these options:**
     - `build/${buildType}` (e.g., `build/Debug`, `build/Release`) - Recommended
     - `build` (single build directory)
   - Additional optional patterns:
     - `cmake-build-${buildType}` (CLion style)
     - Custom pattern

2. **Default build type**: What build type to use when not specified
   - Options: `Release`, `Debug`, `RelWithDebInfo`, `ASAN`, `TSAN`, `MSAN`, `UBSAN`

3. **Default target**: What to build by default
   - Common targets: `clickhouse`, `clickhouse-server`, `clickhouse-client`, or build all

### Test Skill

The `test` skill can be configured with:

1. **Build directory pattern**: Where to find the clickhouse binary for testing
   - Should match the build skill configuration
   - Used to set PATH for test runner
   - **Always prompt with at least:** `build/${buildType}` and `build`

2. **Default build type for testing**: Which build to use when running tests
   - Options: `Release`, `Debug`, `RelWithDebInfo`, `ASAN`, `TSAN`, etc.
   - Typically the same as the build skill default

## Configuration Process

1. **Identify the skill to configure:**
   - If `$ARGUMENTS` is provided, configure that specific skill
   - Otherwise, configure all available skills that need setup (build and test together)

2. **When configuring both build and test skills together:**
   - Ask all questions upfront (build directory, default build type, default target)
   - Use the same answers to configure both skills
   - The test skill will use the same build directory and default build type as the build skill
   - After configuration, update CLAUDE.md to load both skills

3. **For the build skill:**

   a. **Ask about build directory structure:**
   - Use `AskUserQuestion` to ask: "What is your build directory structure?"
   - **MANDATORY:** Always include at least these two options:
     - `build/${buildType}` - Separate directory per build type (e.g., build/Debug, build/Release)
     - `build` - Single build directory for all build types
   - Additional optional options:
     - `cmake-build-${buildType}` - CLion/JetBrains style
     - Custom - Let user specify their own pattern

   b. **Ask about default build type:**
   - Use `AskUserQuestion` to ask: "What should be the default build type?"
   - Options (with descriptions):
     - `RelWithDebInfo` - Optimized with debug info (recommended for development)
     - `Debug` - Full debug symbols, no optimization
     - `Release` - Fully optimized, no debug info
     - `ASAN` - AddressSanitizer build
     - `TSAN` - ThreadSanitizer build
     - Other sanitizers as options

   c. **Ask about default target:**
   - Use `AskUserQuestion` to ask: "What should be the default build target?"
   - Options:
     - `clickhouse` - Main binary (recommended)
     - `clickhouse-server` - Server only
     - `clickhouse-client` - Client only
     - `all` - Build everything
     - Custom - Let user specify

   d. **Update the build skill file:**
   - Read `.claude/skills/build/SKILL.md`
   - Use `Edit` tool to update:
     - Build directory path pattern
     - Default build type in the arguments section
     - Default target in the arguments section
     - Update examples to reflect new defaults
     - Update the "Build Process" section with the actual paths

3. **For the test skill:**

   a. **Ask about build directory structure:**
   - Use the same build directory structure selected for the build skill
   - Or ask separately if configuring test skill independently
   - **MANDATORY:** When asking, always include at least these two options:
     - `build/${buildType}` - Separate directory per build type
     - `build` - Single build directory

   b. **Ask about default build type for testing:**
   - Use `AskUserQuestion` to ask: "Which build should be used for running tests?"
   - Options (with descriptions):
     - `RelWithDebInfo` - Optimized with debug info (recommended, same as build default)
     - `Debug` - Full debug build
     - `Release` - Optimized build
     - `ASAN` - AddressSanitizer build (for memory testing)
     - `TSAN` - ThreadSanitizer build (for concurrency testing)

   c. **Update the test skill file:**
   - Read `.claude/skills/test/SKILL.md`
   - Use `Edit` tool to update:
     - Binary path in step 3 (verify server) to match build directory pattern
     - Binary path in step 4 (PATH export) to match build directory pattern
     - Update examples and notes with correct paths

4. **Confirm configuration:**
   - Show summary of changes made
   - Confirm the skill is now configured for their workspace
   - Provide example commands they can use

5. **Update CLAUDE.md to load configured skills:**

   After configuring skills, update `.claude/CLAUDE.md` to include them in the "Always load and apply" section:

   a. **Read the current CLAUDE.md:**
   - Use `Read` tool to read `.claude/CLAUDE.md`

   b. **Update the skills list:**
   - Find the section that starts with "Always load and apply the following skills:"
   - The section currently lists only `.claude/skills/install-skills`
   - Add entries for all configured skills:
     - `.claude/skills/build` - if build skill was configured
     - `.claude/skills/test` - if test skill was configured

   c. **Use Edit to update CLAUDE.md:**
   - Replace the skills list to include all configured skills
   - Maintain the format with one skill per line prefixed with "- "
   - Example result:
     ```
     Always load and apply the following skills:

     - .claude/skills/install-skills
     - .claude/skills/build
     - .claude/skills/test
     ```

   d. **Confirm the update:**
   - Inform user that CLAUDE.md has been updated to load the configured skills
   - Explain that the skills will be automatically available in future sessions

## Implementation Details

- Always use `AskUserQuestion` to gather preferences before making changes
- Use `Read` to read the current skill files and CLAUDE.md
- Use `Edit` to make precise updates to skill files and CLAUDE.md
- Present clear options with descriptions to help users choose
- **IMPORTANT:** When prompting for build directory, ALWAYS include at minimum: `build/${buildType}` and `build` as options
- Validate user inputs before applying changes
- Show before/after summary of what changed
- After configuring skills, always update CLAUDE.md to include them in the load list

## Example Usage

- `/install-skills` - Configure all skills (build and test together)
- `/install-skills build` - Configure only the build skill
- `/install-skills test` - Configure only the test skill

## Notes

- Configuration is workspace-specific and stored in `.claude/skills/`
- Changes are made to the local skill files
- Users can manually edit skill files later if needed
- The tool preserves other skill content while updating only the specified sections
