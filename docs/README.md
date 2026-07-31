# Contributing to ClickHouse documentation

This folder contains the documentation for ClickHouse.
Content is built with [Mintlify](https://www.mintlify.com/) and hosted at [clickhouse.com/docs](https://clickhouse.com/docs).

<a name="run-docs-locally"/>

## Run the docs site locally

To run the docs locally in order to preview changes, you will need to install the Mintlify CLI tool.
Follow the instructions to [install the CLI](https://www.mintlify.com/docs/cli/install).

You can then run `mint dev` from the `/docs` of this repository to stand up a local development server with hot-reload on `localhost:3000`.

## How to contribute to docs

"Incomplete or confusing documentation" is the top complaint about open source software based on the results of a 2017 [Github Open Source Survey](http://opensourcesurvey.org/2017/).
Documentation is highly valued but often overlooked.
One of the most important contributions someone can make to an open source repository is a documentation update, and we welcome contributions from everyone - including first-time contributors.

To contribute follow the steps below:

1. Fork the repo on GitHub
2. If you only want to make a docs update, clone just what you need. A shallow, sparse clone skips the C++ source and full
   history:

```bash
git clone --depth 1 --filter=blob:none --sparse https://github.com/<your-username>/ClickHouse.git                                 
cd ClickHouse                                                                     
git sparse-checkout set docs ci
```

This gives you the `docs/` and `ci/` folders (plus top-level files) at a fraction  
of the download size.

3. Create a branch off master.
4. Edit or add English content under `docs/` — translations are generated, so there is no need to edit the locale files
5. Preview locally with [`mint dev`](#run-docs-locally) from the `docs/` folder.
6. Push your branch and open a pull request against master
7. A maintainer will review your PR, possibly request changes, and ask you to sign the Contributor License Agreement
8. When CI is green and your PR is ready to ship, a maintainer will merge it, and as a thank you, your name will land in `system.contributors`

If you're a first-time contributor and any of the steps above are confusing,
we recommend [first contributions](https://github.com/firstcontributions/first-contributions#first-contributions)
which walks you through how to make your first contribution.

## Docs folder summary

| Folder | Description |
|--------|-------------|
| `get-started/` | Introductory content: quickstarts, setup, sample datasets, and migration guides |
| `guides/` | How-to guides and use-case documentation for ClickHouse and ClickHouse Cloud |
| `concepts/` | Conceptual topics: core concepts, features, and best practices |
| `reference/` | Reference documentation: SQL functions, data types, table engines, formats, and interfaces |
| `integrations/` | Documentation for integrations: ClickPipes, connectors, and language clients |
| `products/` | Product-specific documentation: Cloud, BYOC, Kubernetes operator, and more |
| `clickstack/` | Documentation for ClickStack, the observability stack |
| `chdb/` | Documentation for chDB, the in-process ClickHouse engine |
| `resources/` | Supporting material: about, changelogs, contribution guides, and support center |
| `changelogs/` | Release changelogs for each ClickHouse version |
| `en/` | Legacy content from the previous (Docusaurus) documentation site — do not add new content here |
| `ar/`, `es/`, `fr/`, `ja/`, `ko/`, `pt-BR/`, `ru/`, `zh/` | Generated translations — do not edit these directly; edit the English content instead |
| `images/` | Image assets used across the documentation |
| `snippets/` | Reusable content snippets that can be imported into multiple pages |
| `_templates/` | Templates for writing new documentation pages (functions, settings, engines, etc.) |
| `_includes/` | Shared content included across multiple pages |
| `_site/` | Site customizations: styles, scripts, logos, and redirects |
| `_migration/` | Tooling used for the migration from the previous documentation site |

Top-level site tabs and languages are configured in `docs.json`, while each section's sidebar entries are defined in its own `navigation.json` file.

## Docs translations

The English language docs function as the source of truth, and we provide AI generated translations of our documentation for the following languages:
- Japanese (`/ja`)
- Korean (`/ko`)
- Spanish (`/es`)
- Brazilian-Portuguese (`/pt-BR`)
- Russian (`/ru`)
- Simplified Chinese (`/zh`)
- Arabic (`/ar`)
- French (`/fr`)

While we do our best to ensure translations are of a high-quality, by maintaining an extensive manually curated glossary and working with a trusted translation provider,
we expect that they may not always be perfect, as LLMs do make mistakes.
We welcome and appreciate corrections to our translated docs from native-speakers in our community.

## Why should you document ClickHouse?

Many developers can say that the code is the best docs by itself, and they are right.
But, ClickHouse is not a project for C++ developers.
Most of its users don't know C++, and they can't understand the code quickly.
ClickHouse is large enough to absorb almost any change without a noticeable trace.
Nobody will find your very useful function, or an important setting, or a very informative new column in a system table if it is not referenced in the documentation.

Here are two common questions:

- "I don't know how to write."

    We have prepared some [recommendations](#common-recommendations) for you.

- "I know what I want to write, but I don't know how to contribute to docs."

    Here are some [tips](#how-to-contribute-to-docs).

Writing the docs is extremely useful for project's users and developers, and grows your karma.

### Documentation for Different Audiences

When writing documentation, think about the people who read it. Each audience has specific requirements for terms they use in communications.

ClickHouse documentation can be divided up by the audience for the following parts:

- Conceptual topics like tutorials and overviews.

    These topics are for the most common audience. When editing text in them, use the most common terms that are comfortable for the audience with basic technical skills.

- Query language reference and related topics.

    These parts of the documentation are dedicated to those who use ClickHouse for data analysis. Carefully describe syntax, input, and output data for expressions. Don't forget the examples.

- Description of table engines and operation details.

    Operation engineers who help data analysts to solve their tasks should know how to install/update a ClickHouse server, maintain the ClickHouse cluster, how to integrate it with other tools and systems, how to get the maximum performance of their entire environment.

- Developer's guides.

    The documentation provides code writers with information about how to write code for ClickHouse and how to build it in different environments.

<a name="common-recommendations"/>

### Common Recommendations

- When searching for a position for your text, try to place it in the most anticipated place.
- Group entities. For example, if several functions solve similar tasks or belong to a specific group by use case or an application type, place them together.
- Try to avoid slang. Use the most common and specific terms possible. If some terms are used as synonyms, state this explicitly.
- Add examples for all the functionality. Add basic examples to show how the function works by itself. Add use case examples to show how the function participates in solving specific tasks.
- Any text concerning politics, religion, or other social related themes are strictly prohibited in all the ClickHouse texts.
- Proofread your text before publishing. Look for typos, missing punctuation, or repetitions that could be avoided.

<a name="templates"/>

### Templates

When writing docs, you can use prepared templates. Copy the code of a template and use it in your contribution. Sometimes you just need to change the level of headers.

Reference templates (paste a section into a page):

- [Function](_templates/template-function.md)
- [Server Setting](_templates/template-server-setting.md)
- [Database or Table engine](_templates/template-engine.md)
- [System table](_templates/template-system-table.md)
- [Statement](_templates/template-statement.md)

Narrative templates (full page):

- [Setup guide](_templates/template-setup-guide.mdx) — how-to and setup guides

#### Choosing components for narrative guides

The section order in a narrative template is fixed; drop a section only if it genuinely doesn't apply. Classify the content's *shape* — the shape dictates the component:

| Content shape | Component |
|---|---|
| Procedure with depth (screenshots, code, sub-steps) | `<Steps>` / `<Step title="…" id="…">` — the `id` makes a step deep-linkable via `#id` |
| A step or section that differs by variant (provider, OS, deployment) | `<Tabs>` / `<Tab title="…" id="…">` — the `id` makes a variant deep-linkable via `#id` |
| Selective-consult content — the reader wants one item (Troubleshooting, FAQ) | `<Accordion title="…">`, one per item |
| Advisory list read in full (Best practices) | `### {#anchor}` subheadings, one per item |
| Matrix — the reader scans a column *down* to compare values (action→result, source→target mappings) | markdown table |
| Callout | `<Note>` / `<Warning>` / `<Tip>` |

Guidelines:

- **How it works** goes *before* the steps and describes the end-to-end *flow* (what happens, in order) — not behavioral or reference detail, which belongs in FAQ.
- The main procedure can be **one or several `## {Task}` sections**.
- **Verify**, **Best practices**, and **FAQ** are prompts as much as sections: include them when there's real content to add; don't fabricate to fill them.
- Frontmatter uses `sidebarTitle` (no repeated `# H1`). Built-in components (`Steps`, `Accordion`, `Tabs`, `Note`, `Warning`, `Tip`) need no import.
- Every `##` / `###` needs a unique `{#anchor}`; `<Step>` and `<Tab>` carry their anchor in an `id` prop.


<a name="how-to-build-docs"/>

## How to Build Documentation

You can build your documentation manually by following the instructions in the docs repo [contrib-writing-guide](https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/contrib-writing-guide.md). Also, our CI runs the documentation build after the `pr-documentation` label is added to PR. You can see the results of a build in the GitHub interface. If you have no permissions to add labels, a reviewer of your PR will add it.
