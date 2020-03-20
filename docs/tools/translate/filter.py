#!/usr/bin/env python3

import os
import random
import sys
import time
import json.decoder
import urllib.parse

import googletrans
import pandocfilters
import requests


translator = googletrans.Translator()
target_language = os.environ.get('TARGET_LANGUAGE', 'ru')
is_debug = os.environ.get('DEBUG') is not None
is_yandex = os.environ.get('YANDEX') is not None


def debug(*args):
    if is_debug:
        print(*args, file=sys.stderr)


def translate(text):
    if target_language == 'en':
        return text
    else:
        if is_yandex:
            text = urllib.parse.quote(text)
            url = f'http://translate.yandex.net/api/v1/tr.json/translate?srv=docs&lang=en-{target_language}&text={text}'
            result = requests.get(url).json()
            debug(result)
            if result.get('code') == 200:
                return result['text'][0]
            else:
                print('Failed to translate', str(result), file=sys.stderr)
                sys.exit(1)
        else:
            time.sleep(random.random())
            return translator.translate(text, target_language).text


def process_buffer(buffer, new_value, item=None):
    if buffer:
        text = ''.join(buffer)

        try:
            translated_text = translate(text)
        except TypeError:
            translated_text = text
        except json.decoder.JSONDecodeError as e:
            print('Failed to translate', str(e), file=sys.stderr)
            sys.exit(1)

        debug('Translate', text, ' -> ', translated_text)

        if text and text[0].isupper() and not translated_text[0].isupper():
            translated_text = translated_text[0].upper() + translated_text[1:]

        if text.startswith(' ') and not translated_text.startswith(' '):
            translated_text = ' ' + translated_text
            
        if text.endswith(' ') and not translated_text.endswith(' '):
            translated_text = translated_text + ' '

        for token in translated_text.split(' '):
            new_value.append(pandocfilters.Str(token))
            new_value.append(pandocfilters.Space())

        if item is None and len(new_value):
            new_value.pop(len(new_value) - 1)
        else:
            new_value[-1] = item
    elif item:
        new_value.append(item)


def process_sentence(value):
    new_value = []
    buffer = []
    for item in value:
        if isinstance(item, list):
            new_value.append([process_sentence(subitem) for subitem in item])
            continue
        elif isinstance(item, dict):
            t = item.get('t')
            c = item.get('c')
            if t == 'Str':
                buffer.append(c)
            elif t == 'Space':
                buffer.append(' ')
            elif t == 'DoubleQuote':
                buffer.append('"')
            else:
                process_buffer(buffer, new_value, item)
                buffer = []
        else:
            new_value.append(item)
    process_buffer(buffer, new_value)
    return new_value


def translate_filter(key, value, _format, _):
    if key not in ['Space', 'Str']:
        debug(key, value)
    try:
        cls = getattr(pandocfilters, key)
    except AttributeError:
        return

    if key == 'Para' and value:
        marker = value[0].get('c')
        if isinstance(marker, str) and marker.startswith('!!!') and len(value) > 2:
            # Admonition case
            if marker != '!!!':
                # Lost space after !!! case
                value.insert(1, pandocfilters.Str(marker[3:]))
                value.insert(1, pandocfilters.Space())
                value[0]['c'] = '!!!'
            admonition_value = []
            remaining_para_value = []
            in_admonition = True
            for item in value:
                if in_admonition:
                    if item.get('t') == 'SoftBreak':
                        in_admonition = False
                    else:
                        admonition_value.append(item)
                else:
                    remaining_para_value.append(item)

            break_value = [pandocfilters.LineBreak(),pandocfilters.Str(' ' * 4)]
            if admonition_value[-1].get('t') == 'Quoted':
                text = process_sentence(admonition_value[-1]['c'][-1])
                text[0]['c'] = '"' + text[0]['c']
                text[-1]['c'] = text[-1]['c'] + '"'
                admonition_value.pop(-1)
                admonition_value += text
            else:
                debug('>>>', )
                text = admonition_value[-1].get('c')
                if text:
                    text = translate(text[0].upper() + text[1:])
                    admonition_value.append(pandocfilters.Space())
                    admonition_value.append(pandocfilters.Str(f'"{text}"'))

            return cls(admonition_value + break_value + process_sentence(remaining_para_value))
        else:
            return cls(process_sentence(value))
    elif key == 'Plain' or key == 'Strong' or key == 'Emph':
        return cls(process_sentence(value))
    elif key == 'Link':
        try:
            # Plain links case
            if value[2][0] == value[1][0].get('c'):
                return pandocfilters.Str(value[2][0])
        except IndexError:
            pass
        value[1] = process_sentence(value[1])
        return cls(*value)
    elif key == 'Header':
        # TODO: title case header in en
        value[2] = process_sentence(value[2])
        return cls(*value)
    elif key == 'SoftBreak':
        return pandocfilters.LineBreak()

    return


if __name__ == "__main__":
    pandocfilters.toJSONFilter(translate_filter)
