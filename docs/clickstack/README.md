# ClickStack documentation {#clickstack-documentation}

`navigation.json` defines the ClickHouse Observability and ClickStack dropdowns. Shared guides and the demo archive appear directly in both edition sidebars; there is no separate common section.

## Page URLs {#page-urls}

Existing guide paths are retained. Open Source pages use the existing `clickstack/` routes. Managed-only pages also retain their existing routes. A shared guide has its original URL in the Open Source navigation and a distinct `clickstack/managed/` counterpart in the Managed navigation, so Mintlify can determine the selected edition from the URL.

Do not list a page URL in both dropdowns: Mintlify selects the first matching edition. Only create a new edition URL when both editions need their own navigation context or different content. The legacy `/clickstack/getting-started` URL explicitly redirects to the OSS quickstart. Without this rule, Mintlify selects the commercial quickstart under the same directory.

## Shared content {#shared-content}

Keep shared content in `docs/snippets/clickstack/shared/`. Edit the snippet to update both editions. Deployment, administration, production, configuration, and management overviews use edition-specific text and smaller shared sections where appropriate.

Links between shared guides use relative URLs resolved from the rendered page URL, not the snippet file location. Preserve matching paths beneath the original and Managed roots. Use an explicit URL for a page available only in one edition. Small snippets reused at different URL depths receive explicit link arguments. The home-page integration grid receives its edition root as `editionRoot` so integration links stay within the selected edition.

Use Mintlify's `Frame` and `img` in snippets. Each snippet declares its own imports; do not rely on imports from its parent page.

Managed onboarding pages and localized pages retain their existing paths and workflows.

## Product naming {#product-naming}

This branch uses **ClickHouse Observability** for the managed product in ClickHouse Cloud and **ClickStack** for the open-source product. Use “open source” as a description when needed, rather than as part of the product name. Historical demo recordings retain the names used at the time.

Product display names do not determine URL paths, file names, anchors, environment variables, or API identifiers. Existing `clickstack` and `managed` paths remain stable through the rename. Update navigation labels, page metadata, and current prose together when changing a product name.

## Edition labels {#edition-labels}

The sidebar product switcher lists ClickHouse Observability first and ClickStack second. Each edition owns its navigation structure. Keep uncertain shared guides in the commercial navigation until their applicability is reviewed.

`docs/_site/customizations/clickstack-edition.js` adds applicability pills above the title, including generated API pages. Commercial shared routes use `/clickstack/managed/`; the script lists commercial-only pages that retain original URLs. Update that list when adding a commercial-only page outside the managed directory. Shared guides show both `ClickHouse Observability` and `OSS`, regardless of the selected sidebar. Edition-specific pages show only their applicable product. Keep `SHARED_GUIDES` aligned when sharing or splitting a guide. The `/docs` deployment prefix is supported.

The OSS landing page uses `/clickstack/home`: Mintlify resolves the section root `/clickstack` to the first (commercial) edition. Other existing OSS page URLs stay unchanged.
