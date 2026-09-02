#!/usr/bin/env python3
"""Validate navigation links inside localized components and page data.

The published locale pages render JSX components (QuickStartsGrid,
SampleDatasetExplorer, KBExplorer, ...) and MDX `export const` data whose card
navigation lives in `href:`/`to:` string literals -- not markdown links. lychee
neither sees `snippets/<locale>/...` nor parses JSX/JS, so `--mode locale-links`
cannot catch when a localized component routes users to the wrong place.

The global navbar customization is also checked because it renders on every
locale tree but lives outside the locale directories scanned below.

For every localized file (under `<locale>/` and `snippets/<locale>/`), check:

  * static `href`/`to` paths: one already under `/<locale>/` must resolve; an
    unprefixed path whose localized counterpart `/<locale>/...` EXISTS is a
    regression (routes readers to English instead of the localized page; if no
    localized counterpart or fragment exists, English is an acceptable
    fallback);
  * template-literal href bases, e.g. `` `/get-started/quickstarts/${id}` `` --
    the fallback the "featured" cards render -- flagged when localized pages
    exist under the base (GT copies the English base verbatim into every locale);
  * `image`/`img`/`src` asset refs to /images or /assets must exist on disk
    (catches stale JS data left over from an old English structure).

`--fix` rewrites the href/template regressions to the localized path. Asset
issues are report-only (a broken image needs a content decision). Without --fix
the script only reports and exits non-zero when violations remain.
"""
import argparse
import json
import os
import re
import sys

LOCALE_DIRS = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"]
EXTS = (".mdx", ".md", ".jsx", ".tsx", ".js")
# Non-page asset/base paths that are legitimately unprefixed. `/docs/` is the
# production mount some shared components hardcode (identically in English), not
# a repo-relative doc path, so it is out of scope for the locale check.
SKIP_PREFIXES = ("/images/", "/assets/", "/_site/", "/.well-known/", "/docs/")
SKIP_EXACT = {"/docs", "/"}
# `href: "/x"`, `href="/x"`, `href={'/x'}`, `to: "/x"`, ...; a `$` belongs
# to a template literal handled by `TEMPLATE`, not a static href.
HREF = re.compile(r"""\b(?:href|to)\s*[:=]\s*\{?\s*(['"`])(/[^'"`\s$]+)\1""")
# A template literal whose static prefix is a doc path, e.g.
# `` `/get-started/quickstarts/${f.id}` `` -- the href fallbacks in
# QuickStartsGrid/KBExplorer. lychee and the static HREF pattern both miss these,
# yet they are exactly what the "featured" cards render. Capture the static base.
TEMPLATE = re.compile(r"`(/[A-Za-z0-9][A-Za-z0-9/_.#-]*)\$\{")
# `image:`/`img=`/`src=` asset refs to /images or /assets. These live in JS data
# (e.g. a stale `featuredQuickstarts` image left over from an old English
# structure) that lychee never checks; the referenced file must exist on disk.
ASSET = re.compile(r"""\b(?:image|img|src)\s*[:=]\s*\{?\s*(['"`])(/(?:images|assets)/[^'"`\s]+)\1""")
EXPLICIT_ANCHOR = re.compile(r"""\{#([^}\s]+)\}|\bid\s*=\s*['"]([^'"]+)['"]""")
SAMPLE_EXPLORER = os.path.join(
    "components", "SampleDatasetExplorer", "SampleDatasetExplorer.jsx"
)
THEME_IMAGE_REQUIRED = (
    "className={`sde-theme-image ${className || ",
    'role="img"',
    'aria-label={item.title}',
    "--sde-image-light-mode",
    "--sde-image-dark-mode",
    "background-image: var(--sde-image-light-mode);",
    ".dark .sde-root .sde-theme-image",
    "background-image: var(--sde-image-dark-mode);",
)
THEME_IMAGE_FORBIDDEN = (
    "prefers-color-scheme",
    "isDark",
    "imageForTheme",
)
NAVBAR_SIGN_IN_LABELS = {
    "ar": "تسجيل الدخول",
    "es": "Iniciar sesión",
    "fr": "Se connecter",
    "ja": "ログイン",
    "ko": "로그인",
    "pt-BR": "Entrar",
    "ru": "Войти",
    "zh": "登录",
}
SIDEBAR_AD_COPY = {
    "en": {
        "dismissLabel": "Dismiss ClickHouse Cloud advert permanently",
        "title": "Try ClickHouse Cloud for FREE",
        "description": (
            "Separation of storage and compute, automatic scaling, built-in "
            "SQL console, and lots more. $300 in free credits when signing up."
        ),
        "linkLabel": "Try it for Free",
    },
    "ar": {
        "dismissLabel": "إخفاء إعلان ClickHouse Cloud نهائيًا",
        "title": "جرّب ClickHouse Cloud مجانًا",
        "description": (
            "فصل التخزين عن الحوسبة، والتوسّع التلقائي، "
            "ووحدة تحكم SQL مضمّنة، وغير ذلك الكثير. "
            "احصل على رصيد مجاني بقيمة 300 دولار عند التسجيل."
        ),
        "linkLabel": "جرّبه مجانًا",
    },
    "es": {
        "dismissLabel": "Descartar permanentemente el anuncio de ClickHouse Cloud",
        "title": "Prueba ClickHouse Cloud GRATIS",
        "description": (
            "Separación de almacenamiento y cómputo, escalado automático, "
            "consola SQL integrada y mucho más. Obtén 300 USD en créditos "
            "gratis al registrarte."
        ),
        "linkLabel": "Pruébalo gratis",
    },
    "fr": {
        "dismissLabel": "Masquer définitivement l’annonce ClickHouse Cloud",
        "title": "Essayez ClickHouse Cloud GRATUITEMENT",
        "description": (
            "Séparation du stockage et du calcul, mise à l’échelle automatique, "
            "console SQL intégrée et bien plus encore. Recevez 300 $ de crédits "
            "gratuits lors de votre inscription."
        ),
        "linkLabel": "Essayer gratuitement",
    },
    "ja": {
        "dismissLabel": "ClickHouse Cloud の広告を今後表示しない",
        "title": "ClickHouse Cloud を無料でお試しください",
        "description": (
            "ストレージとコンピューティングの分離、自動スケーリング、"
            "組み込み SQL コンソールなどを利用できます。登録時に 300 "
            "ドル分の無料クレジットを進呈します。"
        ),
        "linkLabel": "無料で試す",
    },
    "ko": {
        "dismissLabel": "ClickHouse Cloud 광고를 영구적으로 닫기",
        "title": "ClickHouse Cloud를 무료로 사용해 보세요",
        "description": (
            "스토리지와 컴퓨팅 분리, 자동 확장, 기본 제공 SQL 콘솔 등 "
            "다양한 기능을 제공합니다. 가입하면 300달러의 무료 크레딧을 "
            "받을 수 있습니다."
        ),
        "linkLabel": "무료로 사용해 보기",
    },
    "pt-BR": {
        "dismissLabel": "Dispensar permanentemente o anúncio do ClickHouse Cloud",
        "title": "Experimente o ClickHouse Cloud GRÁTIS",
        "description": (
            "Separação de armazenamento e computação, escalonamento automático, "
            "console SQL integrado e muito mais. Receba US$ 300 em créditos "
            "grátis ao se cadastrar."
        ),
        "linkLabel": "Experimente grátis",
    },
}


def build_targets(docs_root):
    pages = set()
    anchors = {}
    for root, dirs, files in os.walk(docs_root):
        dirs[:] = [d for d in dirs if d not in (".git", "node_modules")]
        for n in files:
            if n.endswith((".mdx", ".md")):
                rel = os.path.relpath(os.path.join(root, n), docs_root)
                page = re.sub(r"\.mdx?$", "", rel)
                pages.add(page)
                content = open(os.path.join(root, n), encoding="utf-8", errors="replace").read()
                anchors[page] = {
                    match.group(1) or match.group(2)
                    for match in EXPLICIT_ANCHOR.finditer(content)
                }
    redirects = set()
    rj = os.path.join(docs_root, "_site", "redirects.json")
    if os.path.isfile(rj):
        for r in json.load(open(rj)):
            s = (r.get("source") or "").strip().strip("/")
            if s:
                redirects.add(s)
    return pages, redirects, anchors


def check_sample_explorer_theme_images(docs_root):
    """Validate the SSR-safe, theme-class-driven image renderer in every copy."""
    components = [os.path.join(docs_root, "snippets", SAMPLE_EXPLORER)]
    components += [
        os.path.join(docs_root, "snippets", loc, SAMPLE_EXPLORER)
        for loc in LOCALE_DIRS
    ]
    violations = []
    for component in components:
        rel = os.path.relpath(component, docs_root)
        source = open(component, encoding="utf-8", errors="replace").read()
        for marker in THEME_IMAGE_REQUIRED:
            if marker not in source:
                violations.append(
                    (rel, marker, "missing-theme-image-renderer", None)
                )
        for marker in THEME_IMAGE_FORBIDDEN:
            if marker in source:
                violations.append(
                    (rel, marker, "stale-theme-image-renderer", None)
                )
    return violations


def check_navbar_sign_in_labels(docs_root):
    """Validate the sign-in label rendered by the global navbar script."""
    path = os.path.join(docs_root, "_site", "customizations", "navbar-cta.js")
    source = open(path, encoding="utf-8", errors="replace").read()
    rel = os.path.relpath(path, docs_root)
    violations = []
    for locale, label in NAVBAR_SIGN_IN_LABELS.items():
        key = f"'{locale}'" if "-" in locale else locale
        marker = f"    {key}: '{label}',"
        if source.count(marker) != 1:
            violations.append((rel, locale, "missing-sign-in-label", label))

    locale_pattern = (
        r"/^\/(?:docs\/)?(ar|es|fr|ja|ko|pt-BR|ru|zh)(?:\/|$)/"
    )
    if source.count(locale_pattern) != 1:
        violations.append(
            (rel, locale_pattern, "missing-navbar-locale-detection", None)
        )
    label_assignment = (
        "signInLink.textContent = SIGN_IN_LABELS[getLocale()] || 'Sign in';"
    )
    if source.count(label_assignment) != 1:
        violations.append(
            (rel, label_assignment, "missing-localized-sign-in", None)
        )
    attributed_hrefs = (
        "var SIGN_IN_HREF = 'https://console.clickhouse.cloud/signIn?loc=docs-nav-signIn-cta';",
        "var CTA_HREF = 'https://clickhouse.cloud/signUp?loc=docs-nav-signUp-cta';",
    )
    for href in attributed_hrefs:
        if source.count(href) != 1:
            violations.append(
                (rel, href, "missing-navbar-attribution", None)
            )
    return violations


def check_sidebar_ad_localization(docs_root):
    """Validate attributed, localized sidebar advert behavior."""
    path = os.path.join(
        docs_root, "_site", "customizations", "cloud-sidebar-ad.js"
    )
    source = open(path, encoding="utf-8", errors="replace").read()
    rel = os.path.relpath(path, docs_root)
    violations = []

    required_markers = (
        "var SIGNUP_HREF = 'https://clickhouse.cloud/signUp?loc=docs-card-banner';",
        "return window.location.pathname.replace(/^\\/docs(?=\\/|$)/, '');",
        "var localeMatch = normalizedPath().match(/^\\/(ar|es|fr|ja|ko|pt-BR)(?:\\/|$)/);",
        "if (/^\\/(?:ru|zh)(?:\\/|$)/.test(path)) return false;",
    )
    for marker in required_markers:
        if source.count(marker) != 1:
            violations.append((rel, marker, "missing-sidebar-ad-rule", None))

    for locale, expected_copy in SIDEBAR_AD_COPY.items():
        key = re.escape(f"'{locale}'" if "-" in locale else locale)
        match = re.search(
            rf"^    {key}: \{{\n(?P<body>.*?)^    \}},$",
            source,
            re.MULTILINE | re.DOTALL,
        )
        if not match:
            violations.append((rel, locale, "missing-sidebar-ad-copy", None))
            continue
        body = match.group("body")
        for field, value in expected_copy.items():
            marker = f"      {field}: '{value}',"
            if body.count(marker) != 1:
                violations.append(
                    (rel, f"{locale}.{field}", "stale-sidebar-ad-copy", value)
                )

    for excluded_locale in ("ru", "zh"):
        if re.search(rf"^    {excluded_locale}: \{{", source, re.MULTILINE):
            violations.append(
                (rel, excluded_locale, "sidebar-ad-must-be-suppressed", None)
            )
    return violations


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("docs_root", nargs="?", default=".")
    p.add_argument("--fix", action="store_true", help="Rewrite regressions in place.")
    args = p.parse_args(argv)
    docs_root = os.path.abspath(args.docs_root)
    pages, redirects, anchors = build_targets(docs_root)

    def resolves(bare):
        return bare in pages or (bare + "/index") in pages or bare in redirects

    def has_fragment(bare, fragment):
        return (
            fragment in anchors.get(bare, set())
            or fragment in anchors.get(bare + "/index", set())
        )

    violations = check_sample_explorer_theme_images(docs_root)
    violations += check_navbar_sign_in_labels(docs_root)
    violations += check_sidebar_ad_localization(docs_root)
    # Entries are (file, path or marker, kind, suggestion).
    fixed = 0
    for loc in LOCALE_DIRS:
        roots = [os.path.join(docs_root, loc),
                 os.path.join(docs_root, "snippets", loc)]
        for base in roots:
            for root, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in (".git", "node_modules")]
                for n in files:
                    if not n.endswith(EXTS):
                        continue
                    fp = os.path.join(root, n)
                    rel = os.path.relpath(fp, docs_root)
                    s = open(fp, encoding="utf-8", errors="replace").read()

                    def check(m):
                        nonlocal fixed
                        path = m.group(2)
                        raw = path
                        path, _, fragment = path.partition("#")
                        path = path.split("?")[0]
                        if (path in SKIP_EXACT or path.startswith(SKIP_PREFIXES)):
                            return m.group(0)
                        bare = path.lstrip("/")
                        seg = bare.split("/")
                        if seg and seg[0] == loc:
                            # already localized -- must resolve
                            if not resolves(bare):
                                violations.append((rel, raw, "broken-localized", None))
                            return m.group(0)
                        # unprefixed: localized counterpart exists => must localize
                        localized = f"{loc}/{bare}"
                        english_resolves = resolves(bare)
                        localized_resolves = resolves(localized)
                        if fragment:
                            english_fragment = (
                                english_resolves and has_fragment(bare, fragment)
                            )
                            localized_fragment = (
                                localized_resolves
                                and has_fragment(localized, fragment)
                            )
                            # Do not let --fix preserve a typo on a localized URL.
                            # Localize only when that target actually has the anchor.
                            if not english_fragment and not localized_fragment:
                                violations.append((rel, raw, "broken-fragment", None))
                                return m.group(0)
                            # Keep an English fallback when the page is translated
                            # but the linked section is not.
                            if english_fragment and not localized_fragment:
                                return m.group(0)
                        if localized_resolves:
                            suggestion = "/" + loc + raw  # keep fragment
                            violations.append((rel, raw, "should-localize", suggestion))
                            if args.fix:
                                fixed += 1
                                return m.group(0).replace(raw, "/" + loc + raw, 1)
                            return m.group(0)
                        if not english_resolves:
                            violations.append((rel, raw, "broken", None))
                        return m.group(0)

                    def check_template(m):
                        nonlocal fixed
                        tbase = m.group(1).rstrip("/")
                        if tbase in SKIP_EXACT or tbase.startswith(SKIP_PREFIXES):
                            return m.group(0)
                        bare = tbase.lstrip("/")
                        if bare.split("/")[0] == loc:
                            return m.group(0)  # base already localized
                        # A template building doc URLs from a dynamic id: flag only
                        # when localized pages actually exist under this base (so an
                        # English-only section stays an acceptable fallback). The
                        # per-id target can't be resolved statically; the base is
                        # what routes locale readers to English.
                        if not any(p.startswith(f"{loc}/{bare}/") for p in pages):
                            return m.group(0)
                        violations.append(
                            (rel, m.group(1), "should-localize-template",
                             "/" + loc + m.group(1)))
                        if args.fix:
                            fixed += 1
                            return "`/" + loc + m.group(1) + "${"
                        return m.group(0)

                    ns = TEMPLATE.sub(check_template, HREF.sub(check, s))
                    if args.fix and ns != s:
                        open(fp, "w", encoding="utf-8").write(ns)

                    # Asset refs are not rewritten (a broken image needs a content
                    # decision, not a mechanical fix) -- report only.
                    for am in ASSET.finditer(s):
                        ap = am.group(2).split("#")[0].split("?")[0]
                        if not os.path.exists(os.path.join(docs_root, ap.lstrip("/"))):
                            violations.append((rel, am.group(2), "broken-asset", None))

    kinds = {}
    for _, _, k, _ in violations:
        kinds[k] = kinds.get(k, 0) + 1
    if args.fix:
        print(f"fixed (localized): {fixed}")
    FIXABLE = {"should-localize", "should-localize-template"}
    remaining = [v for v in violations if not (args.fix and v[2] in FIXABLE)]
    print(f"violations: {len(remaining)}  by kind: {kinds}")
    for rel, raw, k, sug in remaining[:40]:
        print(f"  [{k}] {raw}  in {rel}" + (f"  -> {sug}" if sug else ""))
    return 0 if not remaining else 1


if __name__ == "__main__":
    sys.exit(main())
