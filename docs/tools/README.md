## How ClickHouse documentation is generated?

ClickHouse documentation is built using [build.py](build.py) script that uses [mkdocs](https://www.mkdocs.org) library and it's dependencies to separately build all version of documentations (all languages in either single and multi page mode) as static HTMLs. The results are then put in correct directory structure. It can also generate PDF version.

[release.sh](release.sh) also pulls static files needed for [official ClickHouse website](https://clickhouse.yandex) from [../../website](../../website) folder, packs them alongside docs into Docker container and tries to deploy it (possible only from Yandex private network).

## How to check if the documentation will look fine?

There are few options that are all useful depending on how large or complex your edits are.

### Install Markdown editor or plugin for your IDE

Usually those have some way to preview how Markdown will look like, which allows to catch basic errors like unclosed tags very early.

### Use build.py

It'll take some effort to go through, but the result will be very close to production documentation.

For the first time you'll need to install [wkhtmltopdf](https://wkhtmltopdf.org/) and set up virtualenv:

``` bash
$ cd ClickHouse/docs/tools
$ mkdir venv
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

Then running `build.py` without args (there are some, check `build.py --help`) will generate `ClickHouse/docs/build` folder with complete static html website.

You can just directly open those HTML files in browser, but usually it is more convenient to have some sort of HTTP server hosting them. For example, you can launch one by running `cd ClickHouse/docs/build && python -m SimpleHTTPServer 8888` and then go to <http://localhost:8888> in browser.


### Commit blindly

Then push to GitHub so you can use it's preview. It's better to use previous methods too though.

## How to subscribe on documentation changes?

At the moment there's no easy way to do just that, but you can consider:

* Hit the "Watch" button on top of GitHub web interface to know as early as possible, even during pull request.
* Some search engines allow to subscribe on specific website changes via email and you can opt-in for that for <https://clickhouse.yandex>.


