## How ClickHouse documentation is generated? {#how-clickhouse-documentation-is-generated}

ClickHouse documentation is built using [build.py](build.py) script that uses [mkdocs](https://www.mkdocs.org) library and it’s dependencies to separately build all version of documentations (all languages in either single and multi page mode) as static HTMLs for each single page version. The results are then put in the correct directory structure. It is recommended to use Python 3.7 to run this script.

[release.sh](release.sh) also pulls static files needed for [official ClickHouse website](https://clickhouse.com) from [../../website](../../website) folder then pushes to specified GitHub repo to be served via [GitHub Pages](https://pages.github.com).

## How to check if the documentation will look fine? {#how-to-check-if-the-documentation-will-look-fine}

There are few options that are all useful depending on how large or complex your edits are.

### Use GitHub web interface to edit

GitHub has Markdown support with preview feature, but the details of GitHub Markdown dialect are a bit different in ClickHouse documentation.

### Install Markdown editor or plugin for your IDE {#install-markdown-editor-or-plugin-for-your-ide}

Usually those also have some way to preview how Markdown will look like, which allows to catch basic errors like unclosed tags very early.

### Use build.py {#use-build-py}

It’ll take some effort to go through, but the result will be very close to production documentation.

For the first time you’ll need to:

#### 1. Install CLI tools from npm

1. `sudo apt-get install npm` for Debian/Ubuntu or `brew install npm` on Mac OS X.
2. `sudo npm install -g purify-css amphtml-validator`.

#### 2. Set up virtualenv

``` bash
$ cd ClickHouse/docs/tools
$ mkdir venv
$ virtualenv -p $(which python3) venv
$ source venv/bin/activate
$ pip3 install -r requirements.txt
```

#### 3. Run build.py

When all prerequisites are installed, running `build.py` without args (there are some, check `build.py --help`) will generate `ClickHouse/docs/build` folder with complete static html website.

The easiest way to see the result is to use `--livereload=8888` argument of build.py. Alternatively, you can manually launch a HTTP server to serve the docs, for example by running `cd ClickHouse/docs/build && python3 -m http.server 8888`. Then go to http://localhost:8888 in browser. Feel free to use any other port instead of 8888.

## How to change code highlighting? {#how-to-change-code-hl}

ClickHouse does not use mkdocs `highlightjs` feature. It uses modified pygments styles instead.
If you want to change code highlighting, edit the `website/css/highlight.css` file.
Currently, an [eighties](https://github.com/idleberg/base16-pygments/blob/master/css/base16-eighties.dark.css) theme
is used.

## How to subscribe on documentation changes? {#how-to-subscribe-on-documentation-changes}

At the moment there’s no easy way to do just that, but you can consider:

-   To hit the “Watch” button on top of GitHub web interface to know as early as possible, even during pull request. Alternative to this is `#github-activity` channel of [public ClickHouse Slack](https://join.slack.com/t/clickhousedb/shared_invite/zt-qfort0u8-TWqK4wIP0YSdoDE0btKa1w).
-   Some search engines allow to subscribe on specific website changes via email and you can opt-in for that for https://clickhouse.com.
