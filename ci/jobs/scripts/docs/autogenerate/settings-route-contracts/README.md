# Settings route contracts

These files preserve only the route definitions needed to keep generated
settings pages stable between runs. A definition records the matching prefix,
matching mode, and destination page for one generated group.

The contracts deliberately do not contain anchor maps, page statistics, or
setting counts. Anchor-to-page mappings are generated from the current source
and published in `docs/_site/customizations/settings-legacy-routes`; the link
checker and the site use those mappings directly.

Persisting the smaller route contract prevents an added setting from moving
existing settings between pages or changing historical group labels such as
`keeper_server.socket_*`. The generator rewrites a family contract only when
that family's page topology changes.

Once a contract exists, the generator requires both it and the corresponding
legacy-route JavaScript. Missing either file is an error rather than a reason
to regroup pages from scratch. Loaded routing metadata is also validated so
invalid or ambiguous prefixes, modes, targets, and anchor mappings cannot
silently change the generated page layout.
