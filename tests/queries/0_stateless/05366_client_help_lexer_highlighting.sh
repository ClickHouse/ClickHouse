#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `help <name>` renders the Markdown of `system.documentation` in the terminal and highlights every
# SQL code block it contains.  Documentation snippets are mostly query fragments that the parser
# cannot parse, so the client falls back to lexer-based highlighting, which colors each token by its
# lexer kind and leaves brackets and punctuation in the default color.  Documented in
# src/Client/ClientBaseHelpers.h; the token kinds mirror the Web UI (programs/server/play.html).
#
# Every colored run is replaced by the name of its highlight class, so that the test asserts the
# visible token classes and not the documentation prose.

python3 - "$CLICKHOUSE_LOCAL" <<'PYEOF'
import fcntl, os, pty, re, struct, sys, termios

LOCAL = sys.argv[1]

# The escapes replxx emits for the colors of the lexer-based highlighter.
LEXER = {
    '\x1b[0;1m': 'keyword',                  # bold(DEFAULT)
    '\x1b[0;22;36m': 'identifier',           # CYAN
    '\x1b[0;22;33m': 'function',             # BROWN
    '\x1b[0;38;5;44m': 'quoted_identifier',  # rgb666(0, 4, 4)
    '\x1b[0;38;5;40m': 'number',             # rgb666(0, 4, 0)
    '\x1b[0;22;32m': 'string',               # GREEN
    '\x1b[0;90m': 'comment',                 # GRAY
    '\x1b[0;22;31m': 'error',                # RED
}

# The parser-based highlighter has no color for a comment and none for an invalid token, so a block
# containing one of these was provably highlighted by the lexer.  The absence of the parser's own
# colors would be no proof: a parsed block may contain none of the constructs that they mark.
PROOF = ('\x1b[0;90m', '\x1b[0;22;31m')

ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

def helped(word):
    """Run `help <word>` with a terminal on stdout, because only then is the output highlighted."""
    master, slave = pty.openpty()
    # Fix the window size before the child reads it, so the rendering does not depend on the host.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
    pid = os.fork()
    if pid == 0:
        os.close(master)
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
        for fd in (0, 1, 2):
            os.dup2(slave, fd)
        if slave > 2:
            os.close(slave)
        os.execvp('sh', ['sh', '-c', LOCAL + " --query 'help " + word + "'"])
    os.close(slave)
    out = b''
    while True:
        try:
            chunk = os.read(master, 65536)
        except OSError:  # The terminal was closed because the child exited.
            break
        if not chunk:
            break
        out += chunk
    os.close(master)
    os.waitpid(pid, 0)
    return out.decode('utf-8', 'replace').replace('\r', '')

def code_blocks(text):
    """The contiguous runs of code block lines, which the renderer indents by four spaces."""
    blocks, block = [], []
    for line in text.split('\n'):
        if line.startswith('    '):
            block.append(line[4:])
        elif block:
            blocks.append(block)
            block = []
    if block:
        blocks.append(block)
    return blocks

def marked(line):
    """Every colored run replaced by `<class>`; text in the default color is kept as it is."""
    out, colored, pos = [], False, 0
    for match in ESCAPE.finditer(line):
        if not colored:
            out.append(line[pos:match.start()])
        pos = match.end()
        escape = match.group(0)
        colored = escape.startswith('\x1b[0;')
        if colored:
            out.append('<' + LEXER.get(escape, 'other') + '>')
    if not colored:
        out.append(line[pos:])
    return ''.join(out).rstrip()

# The `Syntax` block of a function is a fragment, so the lexer highlights it: the name is a function
# because `(` follows it, the parameters are identifiers, the example below them is a comment, and the
# brackets and commas of the signature keep the default color.
notlike = helped('notLike')
for syntax in code_blocks(notlike):
    if any(ESCAPE.sub('', line).startswith('notLike(') for line in syntax):
        for line in syntax:
            print(marked(line))

# Every token class of the lexer-based highlighter, as seen in the blocks that it provably highlighted.
# Asserted as presence, so that editing the documentation of these entities cannot change the result.
seen = set()
for text in (notlike, helped('ReplacingMergeTree')):
    for block in code_blocks(text):
        joined = '\n'.join(block)
        if not any(proof in joined for proof in PROOF):
            continue
        for escape in ESCAPE.findall(joined):
            if escape in LEXER:
                seen.add(LEXER[escape])
for name in sorted(set(LEXER.values())):
    print(name, int(name in seen))
PYEOF
