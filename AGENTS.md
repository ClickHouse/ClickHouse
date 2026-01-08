# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the C++ engine; `base/` hosts shared utilities; `programs/` builds the unified `clickhouse` binary (server and client tools).
- `contrib/` vendors third-party libraries; `cmake/` holds build modules; `rust/` contains optional Rust components.
- Tests live in `tests/` (functional and integration); documentation in `docs/`; performance tools in `benchmark/`; helper scripts in `utils/`; container and packaging assets in `docker/` and `packages/`.

## Build, Test, and Development Commands
- Configure debug build: `cmake -S . -B build_debug -D CMAKE_BUILD_TYPE=Debug`.
- Build server/client: `cmake --build build_debug --target clickhouse` (or `ninja -C build_debug clickhouse`).
- Build everything (tests/tools): `cmake --build build_debug`.
- Run the server locally: `./build_debug/programs/clickhouse server --config-file programs/server/config.xml`.
- Run a specific functional test: `PATH=build_debug/programs:$PATH tests/clickhouse-test <pattern>`.

## Coding Style & Naming Conventions
- C++ style enforced by `.clang-format`: 4-space indents, braces on separate lines, spaces around binary operators; follow surrounding code.
- Prefer `const` before the type, `using` aliases for templates, and logical grouping of headers/implementations.
- Run `utils/check-style` and format only touched hunks to keep diffs focused; codespell catches typos.

## Testing Guidelines
- Functional SQL tests reside in `tests/queries/0_stateless`; use five-digit prefixes with descriptive slugs (e.g., `01428_feature.sql`) and generate matching `.reference` outputs.
- Integration and other suites sit under `tests/`; prefer `.sql` over `.sh` unless shell behavior is required.
- Clean up artifacts under the `test` database; tag tests (`-- Tags: no-fasttest`, etc.) when special CI handling is needed.

## Commit & Pull Request Guidelines
- Commit subjects are short and imperative (e.g., `Fix .reference`, `Add cpu time to summary`); keep scope focused.
- Use feature branches; include brief PR descriptions, expected behavior, test coverage, and links to issues. Add documentation updates for user-facing changes.
- Sign the Individual CLA on first contribution. Avoid formatting-only churn; keep changes minimal and intentional.

## Security & Configuration Tips
- The server looks for `config.xml` in the working directory; override with `-C`/`--config-file` when needed.
- Explicitly set `CC`/`CXX` if multiple compilers are installed. Keep build directories separate (e.g., `build_debug`, `build_release`) to avoid cross-contamination.
