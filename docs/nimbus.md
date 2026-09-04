# Project Nimbus

We have recently migrated from Docusaruus to Mintlify with the primary goal of
making our docs agent ready. Working with Mintlify it has become clear that the
platform has major challenges in supporting our docs at scale, and several
incidents, the most recent of which resulted in a full docs outage for 30 minutes,
bring into serious question whether we want to continue to use this vendor.

As a result I am exploring alternative options, including https://nimbus-docs.com/
which is the platform that Cloudflare uses.

In this session we will explore the feasibility of migrating to Nimbus and how
much lift is required.

## Details of the current setup

For the most part, docs live in `docs`. Our current setup supports:
- i18n: docs live in the locale specific directories such as `docs/es`, `docs/fr` etc.
  - Translation is taken care of by an external tool called GeneralTranslation which PRs
  translations of the docs when the English version gets merged.
- We are using Mintlify's https://www.mintlify.com/docs/deploy/multi-repo feature for the ClickHouse private docs
  - These docs are hosted in a separate repository (connected via the Mintlify GitHub app)
  - Mintlify does not currently support translations so these docs are English online
- We use custom heading ids as we often link to sections from our blog and desire stable header ids: https://www.mintlify.com/docs/create/text#custom-heading-ids
- We are using Mintlify's [$ref](https://www.mintlify.com/docs/organize/settings-reference#$ref) to break down navigation,
  although this is not necessary. Generally organising the docs according to structure on disk is the most desirable.
- In the changelogs section we are using https://www.mintlify.com/docs/organize/navigation#dropdowns
- We have made several customisations to the site, these are handled as custom JavaScript at `/Users/sstruw/Desktop/ClickHouse/docs/_site/customizations`
- We have many docs defined in C++ source code. There is a nightlyjob `/Users/sstruw/Desktop/ClickHouse/.github/workflows/nightly_docs_autogen.yml`
  that transforms these to markdown. This is an unideal aspect of the current setup with Mintlify and one we need to address.
  Ideally we want to generate these pages in the site during the build so that it is not necessary to have additional copies.
- .mdx files currently contain a slug frontmatter property, but those are **not** actually used. Mintlify assigns a slug based on disk structure and placement based on navigation.json or docs.json.

## Goals and constraints

1. (!!) What must the POC prove for a go decision? Rank: build time at full scale, i18n feasibility, preview flow, agent surface, visual parity, other.
All of the above are important. We need to prove that this is a viable _replacement_ for Mintlify and quantify how much lift is required

2. What is the decision deadline, and does a Mintlify contract date matter?
This is what we are trying to determine. How quickly can we move off of the platform to Nimbus if it meets our requirements.

3. Which Mintlify failures are we solving, and which would we accept as unsolved: uptime, build scale, vendor lock-in, no build-time generation, translation gaps?

The primary Mintlify failures we are solving for:
- Lack of preview support on forks + no control over when we launch previews or how
- Site speed. Mintlify has huge .rsc bundles and the site is extremely unperformant on safari and on large mdx pages such as https://clickhouse.com/docs/reference/functions/regular-functions/other-functions
- limitations of the multi-repo feature WRT translations. Ideally we can keep English docs close to the source in each separate repo, but store translations for all docs in a single place

What we will accept as unsolved:
- Loss of the visual editor (which currently is unusable)

4. Is this Nimbus versus stay, or are Starlight, Fumadocs, or a Docusaurus return also on the table if Nimbus's churn is too risky?
These are other options we can explore, but Nimbus's focus on AX seems well suited for what we set out to achieve with the Mintlify migration in the first place

5. Who owns the platform after migration: team, on-call, upgrade cadence for a 0.x dependency?
A 3 person docs team owns the paltform after migration

6. Would we contribute upstream to cloudflare/nimbus for gaps like heading attributes and i18n hooks, or carry local patches?
Contribute upstream

## Hosting and operations

7. (!!) Where would the site run: Cloudflare Workers, or a host we already operate such as S3 plus CloudFront, Vercel, Netlify, or nginx? Do we have a Cloudflare account and zone
   for clickhouse.com?

Cloudflare workers is an ideal choice. We already use Cloudflare for our website worker.

8. What is the URL model: clickhouse.com/docs/... behind the existing reverse proxy, or a dedicated host? Who controls that routing and can it split traffic during cutover?
`clickhouse.com/docs` behind an existing reverse proxy: `/Users/sstruw/Desktop/clickhouse-website-worker`

9. How should cutover from Mintlify work: path-based, percentage, locale-based, or big-bang?
Behind a flag and then big bang

10. Any uptime, latency, or geography constraints, for example China access?
We expect 99.99% uptime. This was achieved using Vercel and Docusaurus previously.
We had instant rollback capability for any bad changes pushed.
No restrictions on access from specific locales.

11. Do we need any server-side behaviour: Accept: text/markdown negotiation, auth for private pages, edge redirects beyond static limits? This decides static-only versus a Worker.
We would need a way to stage private changes. We could have a private repository set up which tracks only `docs`.
It could get it's own preview links and we merge the changes into the upstream `ClickHouse/ClickHouse`.
Accept: text/markdown negotiation is desired if possible but having a static site is preferable from a performance perspective.
If we can solve this on the Cloudflare level it will be more preferable.

## Internationalisation

12. (!!) Are all 8 locales required at launch, or can the POC and launch be English-first with locales following? What traffic share do locales carry?
The POC should target all locales so we can accurately benchmark build times and site performance at scale

13. Must the URL scheme stay /docs/<locale>/... with a hidden default locale?
Yes

14. Do we need hreflang alternates, a locale switcher, locale-scoped search, and locale-scoped llms.txt?
Yes. Locale-scoped search can probably be handled on the search integration level.

15. (!!) Has GeneralTranslation confirmed support for a non-Mintlify framework, including anchor ids and redirect generation? Is GT itself a keeper?
Their platform is flexible so it will work with whatever we go with

16. May locale trees lag English structurally, and should a missing translation fall back to English or 404?
A missing translation should fall back to English. Locale trees may lag English.

## Private docs and multi-repo
17. (!!) Confirm the private docs repo is ClickHouse/airgap-docs. Who can read it, how large is it, does it have its own snippets? Must it stay separate, or could it move into the
    monorepo or a submodule?
You can check at `/Users/sstruw/Desktop/airgapped-docs`

18. Does "private" mean unlisted but public, or access-controlled? Authentication changes hosting completely.
The repo is accessible only to those user who have access from the org

19. Does the clickhouse-connect one-way copy arrangement continue, and are there other external contributor repos?
We would ideally settle on a single solution for docs in remote repos

## Content and authoring
20. Is a one-time bulk codemod of all MDX acceptable: component renames, snippet imports to <Render>, stripping slug, fixing .js import specifiers, response and result fence
    languages? Should locale trees be transformed too, or regenerated by GT from English?
A one-time bulk codemod is acceptable. We should work with what currently exists in the locale tree as much as possible.

21. Which Mintlify components are must-haves versus droppable: Tooltip, Columns, Check, Update, and the 21 mode: wide landing pages?
Flag any incompatible components

22. Which frontmatter keys must survive: sidebarTitle, doc_type, keywords, integration, tags? Nimbus rejects unknown keys unless declared.
Flag which keys are incompatible

23. Do we want Nimbus's prose linter, draft tagging, audience flag, and git-derived last-updated, or keep our unused vale and markdownlint intentions?
git-derived last-updated is useful. The rest we can ommit and add later if needed.

24. Is visual parity a POC requirement or a later phase? Any brand constraints such as fonts, the maple-like theme, dark and light image pairs?
We should strive for visual parity. Some deviation is acceptable.

## Navigation, URLs, redirects

25. (!!) Confirm the sidebar should follow disk structure with sidebar.order and sidebar.label frontmatter, retiring the 297 navigation files. Any sections where the sidebar must
    deliberately differ from disk?
If there are sections where the nav order differs from disk structure, flag it.

26. Which URLs must never change, and is an old-sitemap-versus-new-build URL diff a POC deliverable?
As Nimbus also uses disk structure, we should strive for the same URL structure as the current site as much as possible.
Yes, it is a deliverable.

27. Are all 13,748 redirects still needed, or can Docusaurus-era ones be pruned? Any wildcard rules?
We should keep the redirects in /docs/_site/redirects.json

28. Is the client-side settings-anchor redirect layer a hard requirement, or can it become server rules?
It can become server rules. No problem. The important thing is that we retain the redirects.

## Autogenerated documentation
29. (!!) For build-time generation, may the docs build depend on downloading a clickhouse binary, or must generation be pure-text from source? Which version should the docs
    describe: master, latest stable, or both?
Here it gets interesting. Ultimately we want to have versioned reference docs (one version added per month, each with translations)
The docs build would need to be part of praktika CI so it can completely depend on a clickhouse binary.
Implementation of versioning is out of scope for the POC.

30. Should generated pages still be committed for review, grep, and GT translation, or exist only in build output? Build-only means untranslated reference pages unless GT changes.
The system I envisioned with output build artifacts to Cloudflare R2 storage and the docs site will process them using Astros remote loader.
There is a POC here in this vain already: https://github.com/ClickHouse/ClickHouse/pull/115770

31. Keep the AUTOGENERATED region model with hand-written prose around generated blocks, or move to whole-page generation with prose in partials?
Move to a whole-page generation

32. Cloud API: is a pinned in-repo spec fine so the nightly job shrinks to a spec bump? Same question for the remote HyperDX spec used by ClickStack.
It should be a remote spec more ideally.

33. How many separate changelogs exist, and do we need RSS, per-product feeds, or the current dropdown UI?
Modelling our changelogs on what Cloudflare is doing on this page would be great: https://developers.cloudflare.com/changelog/

## Search, AI, integrations
34. (!!) Is static Pagefind acceptable at our size across locales, or do we need hosted search? Which of Inkeep, Kapa, and Mintlify's assistant is canonical and stays?
We would continue with Inkeep + Kapa for now

35. Which third-party scripts must survive: Clarity, GTM, Galaxy analytics, Kapa, Inkeep, the web terminal, the SQL highlighter? Any consent constraints?
Everything must survive

36. Which agent surfaces are required at launch: llms.txt, per-section llms.txt, .md twins, Accept: text/markdown, an MCP server, OG images?
MCP server can be excluded for the POC, we could add this ourselves with Inkeep.
Everything else survives.

## CI/CD and previews
37. (!!) Should build and deploy run in Praktika on self-hosted runners or in GitHub Actions? Where do a full-site build and an 8 GB heap fit?
We would rely on Cloudflare for this: https://developers.cloudflare.com/workers/ci-cd/builds/
CI/CD would merely handle triggering and posting preview links back to the PR
We would need to do the same from other remote repositories.

38. Preview requirements: per-PR URL, per-commit URL, PR comment listing changed pages, fork PR previews, cleanup?
Ideally it works like this:
- forks are supported
- A contributor opens a docs PR and as part of CI if docs checks pass a preview build kicks off
- If a contributor pushes a new commit it cancels the current preview build and restarts
- preview links are stable per PR so the latest commit is always shown
39. Is a full build on every docs PR acceptable at roughly 6 to 10 minutes, or do we need incremental or scoped builds?
- To minimise waiting time we want to build only the docs and what gets built can be context dependent
  For example we never need to build locales in a preview if a user is only making English docs updates, and there is no need to build all versions
- ClickHouse Private changes in that repo also only need an English docs build
40. Which current checks must survive unchanged: lychee links and anchors, redirects, navigation completeness, read-only copies, AUTOGENERATED guard, quickstarts, docs examples?
We're flexible here. Lychee links and anchors can remain, redirects, nav completeness -> falls away if we no longer use docs.json. Read-only copies -> falls away if we have a proper way to build from remote collections
Docs examples should remain
41. How should the server's embedded /docs page and terminal help renderer follow the component change? They downgrade Mintlify tags today, with tests 04601 and 04603.
Continue to function as at present

## POC scope
42. (!!) What content slice: whole English tree, English plus one locale, or representative sections such as settings with autogen, a guide with snippets and tabs, a changelog, and the Cloud API?
Full production site for POC
43. Where should the POC live: inside docs/_site/, a sibling directory, or a separate branch or repo?
Let's build it in the current docs folder but we may move it to a separate repo for testing
44. May the POC depend on Cloudflare-only features, or stay host-neutral until hosting is decided?
Depending on Cloudflare-only features is perfectly acceptable. If Cloudflare is down, we are all screwed in anycase.
45. Success metrics: build time target, page parity, redirect coverage, Core Web Vitals, accessibility, agent-surface checks?
Not sure. Will evaluate based on the outcome.
