"""
Guard for the dnf lock-file boot race in `ci/jobs/install_check.py`.

`test_install` closes the window by waiting for `systemd-tmpfiles-setup.service`
between `docker run --detach` and the install command, retrying the probe until the
bus answers and failing closed if the unit does not start. Nothing else pins that:
the substep only observes eventual install success, which passes whenever the race
does not fire, so moving the wait after the install, dropping the bus probe,
collapsing its retry loop or swallowing its failure would leave every other
assertion green while silently restoring the bug.

The module is read statically the way `test_collect_core_dumps.py` reads
`functional_tests.py` - importing it pulls in praktika, and running anything in it
starts containers. The statements are located by loose substring anchors rather than
by the literal command string, which earlier revisions of the file spelled
differently: a guard that refuses the file's own maintenance is worse than no guard.
"""

import ast
import re
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parent.parent / "jobs"
UNIT = "systemd-tmpfiles-setup.service"


def _shell_script(statement):
    """The `bash -c` script the gate runs, rebuilt from the f-string parts.

    None when the statement does not end in a closed `bash -c '...'` string: anything
    appended after the script's closing quote runs in the outer shell, where a `&`
    detaches the whole `docker exec` and the gate reports the shell's own status.
    """
    call = next(
        (node for node in ast.walk(statement) if isinstance(node, ast.Call)), None
    )
    if call is None or not call.args:
        return None
    text = ""
    for node in ast.walk(call.args[0]):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text += node.value
        elif isinstance(node, ast.FormattedValue):
            text += "{}"
    match = re.fullmatch(r".*?bash -c '(.*)'", text, re.S)
    return match.group(1) if match else None


def _install_loop():
    """Source text and the statements of `test_install`'s per-container loop.

    The module is read, not imported: importing it pulls in praktika, and running
    anything in it starts containers.
    """
    source = (JOBS_DIR / "install_check.py").read_text()
    functions = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "test_install"
    ]
    assert len(functions) == 1, [node.lineno for node in functions]
    loops = [node for node in ast.walk(functions[0]) if isinstance(node, ast.For)]
    assert len(loops) == 1, [node.lineno for node in loops]
    return source, loops[0].body


def _only(source, statements, *tokens):
    """Index of the single loop statement whose source contains every token."""
    found = [
        index
        for index, statement in enumerate(statements)
        if all(
            token in (ast.get_source_segment(source, statement) or "")
            for token in tokens
        )
    ]
    assert len(found) == 1, (tokens, found)
    return found[0]


def test_boot_gate_runs_between_the_container_start_and_the_install():
    source, statements = _install_loop()
    run = _only(source, statements, "docker run", "--detach")
    gate = _only(source, statements, "systemd-tmpfiles-setup.service")
    install = _only(source, statements, "docker exec", "install.sh")
    assert run < gate < install, (run, gate, install)


def test_boot_gate_waits_for_the_bus_and_fails_loudly():
    source, statements = _install_loop()
    gate = statements[_only(source, statements, "systemd-tmpfiles-setup.service")]
    segment = ast.get_source_segment(source, gate)
    # While the bus is down `systemctl` cannot reach systemd at all, so a gate built
    # on `systemctl` alone silently does nothing exactly when the race is live.
    assert "systemctl show -p Version" in segment, segment
    # The probe must be retried, and the gate must not swallow its own failure:
    # either alone still reports success without having waited for the unit.
    assert re.search(r"\b(for|while|until)\b", segment), segment
    assert "sleep" in segment, segment
    assert not re.search(r"\|\|\s*(?:true\b|:)", segment), segment
    # A verb that only inspects the unit, a start merely queued with `--no-block`, or a
    # backgrounded command all return 0 without the unit having run.
    assert re.search(
        r"systemctl start\s+systemd-tmpfiles-setup\.service", segment
    ), segment
    assert not re.search(r"&\s*'", segment), segment
    keywords = [
        keyword
        for call in ast.walk(gate)
        if isinstance(call, ast.Call)
        for keyword in call.keywords
        if keyword.arg == "strict"
    ]
    assert len(keywords) == 1, segment
    assert isinstance(keywords[0].value, ast.Constant), segment
    assert keywords[0].value.value is True, segment
    # `strict=True` only sees the script's own exit status, so the start has to be the
    # command that produces it. Only `&&` may follow: it keeps a failed start visible.
    # `systemctl` permutes options after operands, so `<unit> --no-block` queues the job
    # exactly as `--no-block <unit>` does; the operands must be the unit alone.
    script = _shell_script(gate)
    assert script is not None, segment
    start = re.search(r"systemctl start\b", script)
    assert start is not None, script
    commands = re.split(r"\s*&&\s*", script[start.start() :])
    assert commands[0].split()[:2] == ["systemctl", "start"], script
    assert commands[0].split()[2:] == [UNIT], script
    assert not re.search(r"[;&]|\|\|", " ".join(commands[1:])), script
