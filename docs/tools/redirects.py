import os


def write_redirect_html(out_path, to_url):
    out_dir = os.path.dirname(out_path)
    try:
        os.makedirs(out_dir)
    except OSError:
        pass
    with open(out_path, "w") as f:
        f.write(
            f"""<!--[if IE 6]> Redirect: {to_url} <![endif]-->
<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0; url={to_url}">
        <script type="text/javascript">
            window.location.href = "{to_url}";
        </script>
        <title>Page Redirection</title>
    </head>
    <body>
        If you are not redirected automatically, follow this <a href="{to_url}">link</a>.
    </body>
</html>"""
        )


def build_static_redirects(args):
    for static_redirect in [
        ("benchmark.html", "/benchmark/dbms/"),
        ("benchmark_hardware.html", "/benchmark/hardware/"),
        (
            "tutorial.html",
            "/docs/en/getting_started/tutorial/",
        ),
        (
            "reference_en.html",
            "/docs/en/single/",
        ),
        (
            "reference_ru.html",
            "/docs/ru/single/",
        ),
        (
            "docs/index.html",
            "/docs/en/",
        ),
    ]:
        write_redirect_html(
            os.path.join(args.output_dir, static_redirect[0]), static_redirect[1]
        )
