# Antalya skills

Source-of-truth for project-specific Claude Code skills. Each subdirectory is
one skill, laid out per the Claude Code skills spec:

```
skills/
├── README.md                    (this file)
└── <skill-name>/
    ├── SKILL.md                 (required — YAML frontmatter + body)
    └── assets/ | scripts/ | references/   (optional bundled resources)
```

Skills in this directory are **not** auto-discovered. Claude Code looks for
skills in `.claude/skills/` (and a few other locations — see below). To make
a skill here usable, link it into a `.claude/skills/` directory.

## Linking a skill into `.claude/skills/`

From the repo root (`Altinity-ClickHouse/`), create a symlink from
`.claude/skills/<skill-name>` to the corresponding directory under
`antalya/skills/<skill-name>`:

```bash
cd /path/to/Altinity-ClickHouse
mkdir -p .claude/skills
ln -s ../../antalya/skills/antalya-feature-design .claude/skills/antalya-feature-design
```

The link target is relative so the repo is portable across checkout locations.
Verify it resolves:

```bash
ls -l .claude/skills/antalya-feature-design/SKILL.md
# should print the SKILL.md path with no "No such file" error
```

Claude Code will pick the skill up on its next start. Inside a session, type
`/skills` to confirm it's listed, or just trigger it by describing a matching
task.

## Where else `.claude/skills/` is honored

Claude Code scans these locations for skills, in order:

- `<cwd>/.claude/skills/`
- Any `.claude/skills/` in ancestor directories of `<cwd>`
- `~/.claude/skills/` (user-global)
- Installed plugins

For this repo, linking into the top-level `Altinity-ClickHouse/.claude/skills/`
(next to `CLAUDE.md`) covers every working directory under the repo, including
all worktrees. Linking into a specific worktree's `.claude/skills/` (for
example `antalya/.claude/skills/`) scopes the skill to that worktree.

Prefer the top-level link unless a skill is genuinely worktree-specific.

## Adding a new skill

1. Create `antalya/skills/<skill-name>/SKILL.md` with the required frontmatter:
   ```markdown
   ---
   name: <skill-name>
   description: <one-paragraph trigger hint — this is how Claude decides to invoke the skill>
   ---

   # Body...
   ```
2. Bundle any supporting files under `assets/`, `scripts/`, or `references/`
   inside the skill directory.
3. Link it into `.claude/skills/` as shown above.
4. Commit both the skill source (under `antalya/skills/`) and the symlink
   (under `.claude/skills/`) so other contributors pick it up.

For guidance on writing a good SKILL.md — especially the description field,
which drives triggering — see the upstream skill-creator docs.

## Removing or renaming

Delete or update both the source directory and its symlink. A broken symlink
under `.claude/skills/` will produce a load-time warning from Claude Code.

## Current skills

- **antalya-feature-design** — scaffold or review a ClickHouse / Antalya
  feature design document before implementation.
