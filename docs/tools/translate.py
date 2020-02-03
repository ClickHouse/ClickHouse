#!/usr/bin/env python

from __future__ import print_function
import sys
import pprint

import googletrans
import pandocfilters

translator = googletrans.Translator()

def translate(key, value, format, _):
    if key == 'Str':
        print(value.encode('utf8'), file=sys.stderr)
        return
        [meta, contents] = value
        cls = getattr(pandocfilters, key)
        return cls(meta, translator.translate(contents, dest='es'))

if __name__ == "__main__":
    pandocfilters.toJSONFilter(translate)
