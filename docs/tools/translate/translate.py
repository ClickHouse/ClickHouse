#!/usr/bin/env python3

import os
import random
import re
import sys
import time
import urllib.parse

import googletrans
import requests
import yaml

import typograph_ru


translator = googletrans.Translator()
default_target_language = os.environ.get('TARGET_LANGUAGE', 'ru')
curly_braces_re = re.compile('({[^}]+})')

is_yandex = os.environ.get('YANDEX') is not None


def translate_impl(text, target_language=None):
    target_language = target_language or default_target_language
    if target_language == 'en':
        return text
    elif target_language == 'typograph_ru':
        return typograph_ru.typograph(text)
    elif is_yandex:
        text = text.replace('‘', '\'')
        text = text.replace('’', '\'')
        has_alpha = any([char.isalpha() for char in text])
        if text.isascii() and has_alpha and not text.isupper():
            text = urllib.parse.quote(text)
            url = f'http://translate.yandex.net/api/v1/tr.json/translate?srv=docs&lang=en-{target_language}&text={text}'
            result = requests.get(url).json()
            if result.get('code') == 200:
                return result['text'][0]
            else:
                result = str(result)
                print(f'Failed to translate "{text}": {result}', file=sys.stderr)
                sys.exit(1)
        else:
            return text
    else:
        time.sleep(random.random())
        return translator.translate(text, target_language).text


def translate(text, target_language=None):
    return "".join(
        [
            part
            if part.startswith("{") and part.endswith("}")
            else translate_impl(part, target_language=target_language)
            for part in re.split(curly_braces_re, text)
        ]
    )


def translate_toc(root, lang):
    global is_yandex
    is_yandex = True
    if isinstance(root, dict):
        result = []
        for key, value in root.items():
            key = translate(key, lang) if key != 'hidden' and not key.isupper() else key
            result.append((key, translate_toc(value, lang),))
        return dict(result)
    elif isinstance(root, list):
        return [translate_toc(item, lang) for item in root]
    elif isinstance(root, str):
        return root


def translate_po():
    import babel.messages.pofile
    base_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'website', 'locale')
    for lang in ['en', 'zh', 'es', 'fr', 'ru', 'ja', 'tr', 'fa']:
        po_path = os.path.join(base_dir, lang, 'LC_MESSAGES', 'messages.po')
        with open(po_path, 'r') as f:
            po_file = babel.messages.pofile.read_po(f, locale=lang, domain='messages')
        for item in po_file:
            if not item.string:
                global is_yandex
                is_yandex = True
                item.string = translate(item.id, lang)
        with open(po_path, 'wb') as f:
            babel.messages.pofile.write_po(f, po_file)


if __name__ == '__main__':
    target_language = sys.argv[1]
    if target_language == 'po':
        translate_po()
    else:
        result = translate_toc(yaml.full_load(sys.stdin.read())['nav'], sys.argv[1])
        print(yaml.dump({'nav': result}))
