---
name: valueref-pilot
description: Resume and continue the long-running "ValueRef pilot" project — reducing reliance on the DB::Field class in the ClickHouse server by introducing a non-owning ValueRef (column pointer + row index). Use whenever the user asks to work on, continue, or check status of the Field/ValueRef refactoring, the "Field pilot", "ValueRef", reducing Field usage/materialization on hot paths, or the project stored under .cursor/projects/valueref-pilot/.
disable-model-invocation: false
allowed-tools: Task, Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch, AskUserQuestion
---

# ValueRef pilot — project entry point

This project reduces reliance on `DB::Field` (`src/Core/Field.h`) in the ClickHouse server by
introducing a lightweight, non-owning `ValueRef` (a `const IColumn*` + row index). All project
state, context, plans, and history live as git-tracked files under
`.cursor/projects/valueref-pilot/` — that directory is the single source of truth and is
tool-neutral (readable from both Cursor and Claude Code).

## What to do when this skill is invoked

1. Read `.cursor/projects/valueref-pilot/AGENT_GUIDE.md` **in full** — it is the operating manual
   (git/PR conventions, build & test commands, and the memory-update protocol).
2. Read the newest entry in `.cursor/projects/valueref-pilot/PROGRESS_LOG.md` — "where we left off".
3. Read `.cursor/projects/valueref-pilot/TASKS.md` — pick the next `TODO`/`IN PROGRESS` task.
4. Skim `DESIGN.md` (the intended `ValueRef` shape) and consult `INVESTIGATION.md` (background,
   with file:line references) as needed.

Then continue the work following the guide. Before ending the session, update `TASKS.md` and
append to `PROGRESS_LOG.md` as instructed in `AGENT_GUIDE.md` §3.

## Hard guardrails (also in AGENT_GUIDE.md §6)

- The goal is **reduction of `Field` on hot paths, not removal.** Do not try to delete `Field`.
- Do **not** change SQL-literal representation (`ASTLiteral`), the settings `Field` API, or any
  on-disk/wire format (`partition.dat`, `minmax_*.idx`, skip-index `.idx2`, statistics files,
  `writeFieldBinary` encodings, partition-ID hashing). These are backward-compatibility surfaces.
- Follow the repo `AGENTS.md`: never commit to `master`; branch as `cursor/<name>-f766`; no
  rebase/amend on shared branches; no stacked PRs; Allman braces.
