#!/usr/bin/env python3

import os
import random
import sys
import time
import urllib.parse

import googletrans
import requests
import yaml

import typograph_ru


translator = googletrans.Translator()
target_language = os.environ.get('TARGET_LANGUAGE', 'ru')

is_yandex = os.environ.get('YANDEX') is not None


def translate(text):
    if target_language == 'en':
        return text
    elif target_language == 'typograph_ru':
        return typograph_ru.typograph(text)
    elif is_yandex:
        text = urllib.parse.quote(text)
        url = f'http://translate.yandex.net/api/v1/tr.json/translate?srv=docs&lang=en-{target_language}&text={text}'
        result = requests.get(url).json()
        if result.get('code') == 200:
            return result['text'][0]
        else:
            print('Failed to translate', str(result), file=sys.stderr)
            sys.exit(1)
    else:
        time.sleep(random.random())
        return translator.translate(text, target_language).text


def translate_toc(root):
    if isinstance(root, dict):
        result = []
        for key, value in root.items():
            key = translate(key) if key != 'hidden' and not key.isupper() else key
            result.append((key, translate_toc(value),))
        return dict(result)
    elif isinstance(root, list):
        return [translate_toc(item) for item in root]
    elif isinstance(root, str):
        return root


if __name__ == '__main__':
    target_language = sys.argv[1]
    is_yandex = True
    result = translate_toc(yaml.full_load(sys.stdin.read())['nav'])
    print(yaml.dump({'nav': result}))
