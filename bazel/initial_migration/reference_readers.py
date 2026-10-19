"""Read the configured CMake File API and the matching Ninja build edges."""

from contextlib import ExitStack
import gzip
import json
from pathlib import Path
import shlex
import tarfile


def read_file_api(path, include_target=None):
    """Return (codemodel, targets) from a reply directory or its tar archive.

    A target rejected by include_target is kept as its codemodel entry. LLVM
    generation needs every target ID for dependencies but reads only its own
    target records.
    """
    path = Path(path)
    with ExitStack() as stack:
        if path.is_dir():
            files = {p.name: p for p in path.iterdir() if p.is_file()}

            def read(name):
                return json.loads(files[name].read_text())
        else:
            archive = stack.enter_context(tarfile.open(path))
            files = {Path(m.name).name: m for m in archive if m.isfile()}

            def read(name):
                return json.load(archive.extractfile(files[name]))

        index = read(sorted(n for n in files if n.startswith('index-') and n.endswith('.json'))[-1])
        model = read(next(o['jsonFile'] for o in index['objects'] if o['kind'] == 'codemodel'))
        targets = {
            t['name']: read(t['jsonFile']) if include_target is None or include_target(t['name']) else t
            for t in model['configurations'][0]['targets']
        }
    return model, targets


def ninja_words(text):
    # The captured link reference does not support escaped Ninja separators.
    if '$ ' in text or '$:' in text or '\n' in text.rstrip('\n'):
        raise ValueError('Unsupported Ninja escaping in link reference: ' + text[:240])
    return shlex.split(text.replace('$$', '$'))


def ninja_edges(path, variables=()):
    """Yield build edges in source order, retaining requested Ninja variables.

    COMMAND is always retained, unescaping Ninja's $$. Other variables remain
    raw for callers to interpret, so link flags need not be stored for every
    compilation edge.
    """
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    current = None
    with opener(path, 'rt') as stream:
        for line in stream:
            while line.endswith('$\n'):
                line = line[:-2] + next(stream)
            if line.startswith('build '):
                if current is not None:
                    yield current
                out, sep, rest = line[6:].partition(': ')
                # A single $ escapes the separator instead of ending outputs;
                # $$ at the end of a filename is a literal dollar sign.
                if not sep or (len(out) - len(out.rstrip('$'))) % 2:
                    raise ValueError('Malformed Ninja build edge: ' + line[:240])
                outputs = ninja_words(out)
                boundary = outputs.index('|') if '|' in outputs else len(outputs)
                tokens = ninja_words(rest)
                if not tokens:
                    raise ValueError('Missing Ninja rule: ' + line[:240])
                current = {
                    'outputs': outputs[:boundary],
                    'implicit_outputs': outputs[boundary + 1:],
                    'rule': tokens[0],
                    'explicit_inputs': [],
                    'implicit_inputs': [],
                    'order_only_inputs': [],
                    'validations': [],
                    'variables': {},
                }
                group = 'explicit_inputs'
                for token in tokens[1:]:
                    if token == '|':
                        group = 'implicit_inputs'
                    elif token == '||':
                        group = 'order_only_inputs'
                    elif token == '|@':
                        group = 'validations'
                    else:
                        current[group].append(token)
                current['normal_inputs'] = current['explicit_inputs'] + current['implicit_inputs']
                current['inputs'] = current['normal_inputs'] + current['order_only_inputs']
            elif current is not None and line.startswith('  '):
                key, sep, value = line[2:].rstrip('\n').partition(' = ')
                if sep and key == 'COMMAND':
                    current['command'] = value.replace('$$', '$')
                if sep and key in variables:
                    current['variables'][key] = value
            elif current is not None and line.strip():
                yield current
                current = None
    if current is not None:
        yield current


def read_ninja(path):
    """Index explicit outputs, as consumed by code generation and its audit."""
    return {
        output: node
        for node in ninja_edges(path)
        for output in node['outputs']
    }
