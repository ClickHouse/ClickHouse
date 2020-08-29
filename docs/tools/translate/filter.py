#!/usr/bin/env python3

import os
import sys
import json.decoder

import pandocfilters
import slugify

import translate
import util


is_debug = os.environ.get('DEBUG') is not None

filename = os.getenv('INPUT')


def debug(*args):
    if is_debug:
        print(*args, file=sys.stderr)


def process_buffer(buffer, new_value, item=None, is_header=False):
    if buffer:
        text = ''.join(buffer)

        try:
            translated_text = translate.translate(text)
        except TypeError:
            translated_text = text
        except json.decoder.JSONDecodeError as e:
            print('Failed to translate', str(e), file=sys.stderr)
            sys.exit(1)

        debug(f'Translate: "{text}" -> "{translated_text}"')

        if text and text[0].isupper() and not translated_text[0].isupper():
            translated_text = translated_text[0].upper() + translated_text[1:]

        if text.startswith(' ') and not translated_text.startswith(' '):
            translated_text = ' ' + translated_text

        if text.endswith(' ') and not translated_text.endswith(' '):
            translated_text = translated_text + ' '

        if is_header and translated_text.endswith('.'):
            translated_text = translated_text.rstrip('.')

        title_case = is_header and translate.default_target_language == 'en' and text[0].isupper()
        title_case_whitelist = {
            'a', 'an', 'the', 'and', 'or', 'that',
            'of', 'on', 'for', 'from', 'with', 'to', 'in'
        }
        is_first_iteration = True
        for token in translated_text.split(' '):
            if title_case and token.isascii() and not token.isupper():
                if len(token) > 1 and token.lower() not in title_case_whitelist:
                    token = token[0].upper() + token[1:]
                elif not is_first_iteration:
                    token = token.lower()
            is_first_iteration = False

            new_value.append(pandocfilters.Str(token))
            new_value.append(pandocfilters.Space())

        if item is None and len(new_value):
            new_value.pop(len(new_value) - 1)
        else:
            new_value[-1] = item
    elif item:
        new_value.append(item)


def process_sentence(value, is_header=False):
    new_value = []
    buffer = []
    for item in value:
        if isinstance(item, list):
            new_value.append([process_sentence(subitem, is_header) for subitem in item])
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
                process_buffer(buffer, new_value, item, is_header)
                buffer = []
        else:
            new_value.append(item)
    process_buffer(buffer, new_value, is_header=is_header)
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
            break_value = [pandocfilters.LineBreak(), pandocfilters.Str(' ' * 4)]
            for item in value:
                if in_admonition:
                    if item.get('t') == 'SoftBreak':
                        in_admonition = False
                    else:
                        admonition_value.append(item)
                else:
                    if item.get('t') == 'SoftBreak':
                        remaining_para_value += break_value
                    else:
                        remaining_para_value.append(item)

            if admonition_value[-1].get('t') == 'Quoted':
                text = process_sentence(admonition_value[-1]['c'][-1])
                text[0]['c'] = '"' + text[0]['c']
                text[-1]['c'] = text[-1]['c'] + '"'
                admonition_value.pop(-1)
                admonition_value += text
            else:
                text = admonition_value[-1].get('c')
                if text:
                    text = translate.translate(text[0].upper() + text[1:])
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
        href = value[2][0]
        if not (href.startswith('http') or href.startswith('#')):
            anchor = None
            attempts = 10
            if '#' in href:
                href, anchor = href.split('#', 1)
            if href.endswith('.md') and not href.startswith('/'):
                parts = [part for part in os.environ['INPUT'].split('/') if len(part) == 2]
                lang = parts[-1]
                script_path = os.path.dirname(__file__)
                base_path = os.path.abspath(f'{script_path}/../../{lang}')
                href = os.path.join(
                    os.path.relpath(base_path, os.path.dirname(os.environ['INPUT'])),
                    os.path.relpath(href, base_path)
                )
            if anchor:
                href = f'{href}#{anchor}'
            value[2][0] = href
        return cls(*value)
    elif key == 'Header':
        if value[1][0].islower() and '_' not in value[1][0]:  # Preserve some manually specified anchors
            value[1][0] = slugify.slugify(value[1][0], separator='-', word_boundary=True, save_order=True)

        # TODO: title case header in en
        value[2] = process_sentence(value[2], is_header=True)
        return cls(*value)
    elif key == 'SoftBreak':
        return pandocfilters.LineBreak()

    return


if __name__ == "__main__":
    os.environ['INPUT'] = os.path.abspath(os.environ['INPUT'])
    pwd = os.path.dirname(filename or '.')
    if pwd:
        with util.cd(pwd):
            pandocfilters.toJSONFilter(translate_filter)
    else:
        pandocfilters.toJSONFilter(translate_filter)
