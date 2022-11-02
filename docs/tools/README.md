## Generating ClickHouse documentation {#how-clickhouse-documentation-is-generated}

ClickHouse documentation is built using [Docusaurus](https://docusaurus.io). 

## Check the look of your documentation changes {#how-to-check-if-the-documentation-will-look-fine}

There are a few options that are all useful depending on how large or complex your edits are.

### Use the GitHub web interface to edit

Every page in the docs has an **Edit this page** link that opens the page in the GitHub editor.  GitHub has Markdown support with a preview feature. The details of GitHub Markdown and the documentation Markdown are a bit different but generally this is close enough, and the person merging your PR will build the docs and check them.

### Install a Markdown editor or plugin for your IDE {#install-markdown-editor-or-plugin-for-your-ide}

Usually, these plugins provide a preview of how the markdown will render, and they catch basic errors like unclosed tags very early.


## Build the docs locally {#use-build-py}

You can build the docs locally.  It takes a few minutes to set up, but once you have done it the first time, the process is very simple.

### Clone the repos

The documentation is in two repos, clone both of them:
- [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse)
- [ClickHouse/ClickHouse-docs](https://github.com/ClickHouse/clickhouse-docs)

### Install Node.js

The documentation is built with Docusaurus, which requires Node.js.  We recommend version 16. Install [Node.js](https://nodejs.org/en/download/).

### Copy files into place

Docusaurus expects all of the markdown files to be located in the directory tree `clickhouse-docs/docs/`.  This is not the way our repos are set up, so some copying of files is needed to build the docs:

```bash
# from the parent directory of both the ClickHouse/ClickHouse and ClickHouse-clickhouse-docs repos:
cp -r ClickHouse/docs/en/development     clickhouse-docs/docs/en/
cp -r ClickHouse/docs/en/engines         clickhouse-docs/docs/en/
cp -r ClickHouse/docs/en/getting-started clickhouse-docs/docs/en/
cp -r ClickHouse/docs/en/interfaces      clickhouse-docs/docs/en/
cp -r ClickHouse/docs/en/operations      clickhouse-docs/docs/en/
cp -r ClickHouse/docs/en/sql-reference   clickhouse-docs/docs/en/

cp -r ClickHouse/docs/ru/*               clickhouse-docs/docs/ru/
cp -r ClickHouse/docs/zh                 clickhouse-docs/docs/
```

#### Note: Symlinks will not work.
### Setup Docusaurus

There are two commands that you may need to use with Docusaurus:
- `yarn install`
- `yarn start`

#### Install Docusaurus and its dependencies:

```bash
cd clickhouse-docs
yarn install
```

#### Start a development Docusaurus environment

This command will start Docusaurus in development mode, which means that as you edit source (for example, `.md` files) files the changes will be rendered into HTML files and served by the Docusaurus development server.

```bash
yarn start
```

### Make your changes to the markdown files

Edit your files.  Remember that if you are editing files in the `ClickHouse/ClickHouse` repo then you should edit them
in that repo and then copy the edited file into the `ClickHouse/clickhouse-docs/` directory structure so that they are updated in your develoment environment.

`yarn start` probably opened a browser for you when you ran it; if not, open a browser to `http://localhost:3000/docs/en/intro` and navigate to the documentation that you are changing.  If you have already made the changes, you can verify them here; if not, make them, and you will see the page update as you save the changes.  

## How to change code highlighting? {#how-to-change-code-hl}

Code highlighting is based on the language chosen for your code blocks.  Specify the language when you start the code block:

<pre lang="no-highlight"><code>```sql
SELECT firstname from imdb.actors;
```
</code></pre>

```sql
SELECT firstname from imdb.actors;
```

If you need a language supported then open an issue in [ClickHouse-docs](https://github.com/ClickHouse/clickhouse-docs/issues).
## How to subscribe on documentation changes? {#how-to-subscribe-on-documentation-changes}

At the moment there’s no easy way to do just that, but you can consider:

-   To hit the “Watch” button on top of GitHub web interface to know as early as possible, even during pull request. Alternative to this is `#github-activity` channel of [public ClickHouse Slack](https://join.slack.com/t/clickhousedb/shared_invite/zt-qfort0u8-TWqK4wIP0YSdoDE0btKa1w).
-   Some search engines allow to subscribe on specific website changes via email and you can opt-in for that for https://clickhouse.com.
