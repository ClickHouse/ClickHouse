import os


def write_redirect_html(out_path, to_url):
    out_dir = os.path.dirname(out_path)
    try:
        os.makedirs(out_dir)
    except OSError:
        pass
    with open(out_path, 'w') as f:
        f.write(f'''<!--[if IE 6]> Redirect: {to_url} <![endif]-->
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
</html>''')


def build_redirect_html(args, base_prefix, lang, output_dir, from_path, to_path):
    out_path = os.path.join(
        output_dir, lang,
        from_path.replace('/index.md', '/index.html').replace('.md', '/index.html')
    )
    target_path = to_path.replace('/index.md', '/').replace('.md', '/')
    to_url = f'/{base_prefix}/{lang}/{target_path}'
    to_url = to_url.strip()
    write_redirect_html(out_path, to_url)


def build_docs_redirects(args):
    with open(os.path.join(args.docs_dir, 'redirects.txt'), 'r') as f:
        for line in f:
            for lang in args.lang.split(','):
                from_path, to_path = line.split(' ', 1)
                build_redirect_html(args, 'docs', lang, args.docs_output_dir, from_path, to_path)


def build_blog_redirects(args):
    for lang in args.blog_lang.split(','):
        redirects_path = os.path.join(args.blog_dir, lang, 'redirects.txt')
        if os.path.exists(redirects_path):
            with open(redirects_path, 'r') as f:
                for line in f:
                    from_path, to_path = line.split(' ', 1)
                    build_redirect_html(args, 'blog', lang, args.blog_output_dir, from_path, to_path)


def build_static_redirects(args):
    for static_redirect in [
        ('benchmark.html', '/benchmark/dbms/'),
        ('benchmark_hardware.html', '/benchmark/hardware/'),
        ('tutorial.html', '/docs/en/getting_started/tutorial/',),
        ('reference_en.html', '/docs/en/single/', ),
        ('reference_ru.html', '/docs/ru/single/',),
        ('docs/index.html', '/docs/en/',),
    ]:
        write_redirect_html(
            os.path.join(args.output_dir, static_redirect[0]),
            static_redirect[1]
        )
