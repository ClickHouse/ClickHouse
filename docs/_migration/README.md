# Migration

One-shot tooling and data used to migrate the ClickHouse docs from Docusaurus
to Mintlify. **This entire folder can be removed once the migration is
complete** — nothing in the live site depends on it at runtime.

## Contents

- `slug-map.csv` — pairing of every Docusaurus slug → Mintlify file/URL, with
  source/migrated hashes for staleness tracking.
- `slug-aliases.csv` — manually-reviewed aliases for slugs that couldn't be
  resolved automatically.
- `migrate.py` — main migration script. Reads `slug-map.csv`, applies
  Mintlify-necessary transforms, writes the result.
- `generate-slug-map.py` — regenerates `slug-map.csv` from upstream Docusaurus
  + this repo's current layout.
- `apply-slug-aliases.py` — rewrites `<!-- MIGRATE: unknown slug -->` markers
  using `slug-aliases.csv`.
- `suggest-slug-aliases.py` — proposes alias candidates for unresolved slugs.
- `match_slugless.py` — finds Mintlify pages with no upstream slug match.
- `verify_mapping.py` — sanity-checks the slug map for duplicates/drift.
- `find_dup_imports.py` — detects MDX import conflicts caused by Mintlify
  hoisting snippet imports into the parent bundle.

See `.claude/skills/migrate-docusaurus-to-mintlify/SKILL.md` for the
migration workflow.