# Progress log

Append-only journal. Newest entry on top. Never edit or delete past entries — correct them with a
new entry. Each entry: date, who/what environment, what changed, decisions + rationale, any
numbers, and an explicit "next step". See `AGENT_GUIDE.md` §3.

---

## 2026-07-10 — Session 0: project bootstrapped (Cursor cloud agent)

**What happened**
- Completed the initial investigation into removing/reducing `Field`. Findings captured in
  `INVESTIGATION.md`. Headline: full removal is infeasible; targeted reduction on hot paths via a
  non-owning `ValueRef` is the plan.
- Created this persistent project memory under `.cursor/projects/valueref-pilot/`
  (`README`, `AGENT_GUIDE`, `INVESTIGATION`, `DESIGN`, `ROADMAP`, `TASKS`, `PROGRESS_LOG`).
- Added `.claude/skills/valueref-pilot/SKILL.md` so Claude Code on a Linux console auto-discovers
  the project and is routed into this memory. Both Cursor and Claude read git-tracked files, so
  the memory itself is tool-neutral; the skill is just the discovery hook for the console agent.
- Branch: `cursor/valueref-pilot-infrastructure-f766`, off `master`. Draft PR opened (see PR link
  once created).

**Key decisions**
- Memory lives in git (not agent-ephemeral) so it survives across sessions and environments.
- Pilot scope is *reduction*, not removal. `Field` stays for SQL literals, settings, and on-disk/
  wire formats — these are explicitly out of scope (see `AGENT_GUIDE.md` §6, `INVESTIGATION.md`
  §B/§C).
- `ValueRef` = `{const IColumn*, size_t row}`, non-owning, `Field`-free; `toField()` is only an
  escape hatch. Rationale: preserves exact SQL type (fixes the `NearestFieldType` collapse) and
  avoids per-value allocation.
- Recommended first migration target is a single hot call site — `SingleValueDataGeneric`
  (lower risk) or `KeyCondition` min/max (higher value). Decide at the start of Phase 2.

**Numbers**
- None yet. `sizeof(DB::Field)` is documented as ~40 bytes (libc++) in `Field.h`; confirm and
  record `sizeof(ValueRef)` in Phase 1.

**Next step**
- Push branch and open the draft PR (Phase 0 exit).
- Then start Phase 1: implement `ValueRef` + `gtest_value_ref.cpp`. See `TASKS.md`.

---
