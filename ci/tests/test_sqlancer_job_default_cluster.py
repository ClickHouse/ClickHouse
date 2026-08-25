"""
Contract test for the `default` cluster drop-in in `ci/jobs/sqlancer_job.sh`.

Several SQLancer oracles read through a cluster named `default`
(`cluster('default', ...)` and `ENGINE = Distributed('default', ...)`). The job
starts its server with no `--config-file`, so the server reads the binary's
embedded config, which defines no `remote_servers` at all - every such read then
fails with `CLUSTER_DOESNT_EXIST`, and the oracle harness turns that into an
`AssertionError` that kills the worker thread. That is invisible to any
functional test: nothing in the repository exercises this job's server
configuration, and the failure only appears in a nightly run.

Two couplings keep it fixed, and each is silent in a different way if broken:

1. the drop-in must define a cluster literally named `default`. A cluster under
   any other name leaves the reads failing exactly as before, and the file still
   parses as valid XML, so neither the shell nor the server complains.
2. the replica port must match `<tcp_port>` in `programs/server/embedded.xml`.
   A mismatch is not silent - the reads fail with a connection error instead -
   but it fails as a nightly-only red, and the two files carry the number
   independently, so nothing else ties them together.

The assertions extract the real heredoc from `sqlancer_job.sh` and execute it
under bash, so the XML under test is the byte sequence the job actually writes
rather than a copy of it, and the port is read from the real `embedded.xml`.
"""

import os
import subprocess
import xml.etree.ElementTree as ET

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_JOB = os.path.join(_ROOT, "ci", "jobs", "sqlancer_job.sh")
_EMBEDDED = os.path.join(_ROOT, "programs", "server", "embedded.xml")

_DROPIN = "zz_ci_default_cluster.xml"


def _emit_dropin(tmp_path):
    """Run the job's own heredoc for the drop-in and return the written text.

    The statement is located by the path it writes and delimited by the heredoc
    terminator, then executed with `SERVER_DIR` pointed at a temporary tree - so
    a drift in the emitted XML, or a shell error inside the heredoc, surfaces
    here rather than in a nightly run.
    """
    lines = open(_JOB, encoding="utf-8").read().splitlines()
    starts = [
        i for i, line in enumerate(lines) if line.startswith(f'cat > "$SERVER_DIR/config.d/{_DROPIN}"')
    ]
    assert len(starts) == 1, f"expected exactly one heredoc writing {_DROPIN}, found {len(starts)}"
    start = starts[0]
    ends = [i for i in range(start + 1, len(lines)) if lines[i] == "XML"]
    assert ends, f"unterminated heredoc for {_DROPIN}"
    snippet = "\n".join(lines[start : ends[0] + 1])

    server_dir = os.path.join(tmp_path, "server")
    os.makedirs(os.path.join(server_dir, "config.d"))
    subprocess.run(
        ["bash", "-eu", "-o", "pipefail", "-c", snippet],
        env={**os.environ, "SERVER_DIR": server_dir},
        check=True,
    )
    return open(os.path.join(server_dir, "config.d", _DROPIN), encoding="utf-8").read()


def _replicas(dropin_text):
    root = ET.fromstring(dropin_text)
    servers = root.find("remote_servers")
    assert servers is not None, "drop-in defines no <remote_servers>"
    return {
        name.tag: [
            (replica.findtext("host"), replica.findtext("port"))
            for shard in name.findall("shard")
            for replica in shard.findall("replica")
        ]
        for name in servers
    }


def test_dropin_defines_a_cluster_named_default(tmp_path):
    # The oracles name the cluster literally; any other name leaves every
    # cluster()/Distributed() read failing with CLUSTER_DOESNT_EXIST.
    clusters = _replicas(_emit_dropin(str(tmp_path)))
    assert "default" in clusters, f"no `default` cluster defined, got {sorted(clusters)}"


def test_dropin_replica_port_matches_embedded_config(tmp_path):
    # The server takes its listening port from the embedded config, and the
    # drop-in repeats it; a mismatch turns every such read into a connection
    # error.
    tcp_port = ET.parse(_EMBEDDED).getroot().findtext("tcp_port")
    assert tcp_port, "embedded.xml declares no <tcp_port>"
    replicas = _replicas(_emit_dropin(str(tmp_path)))["default"]
    assert replicas, "`default` cluster defines no replicas"
    for host, port in replicas:
        assert port == tcp_port, f"replica {host} uses port {port}, embedded.xml listens on {tcp_port}"


def test_dropin_sorts_after_the_provider_overrides():
    # The provider ships its own config.d files and may later ship one naming
    # `remote_servers`; ClickHouse merges config.d in lexicographic order, so
    # the `zz_` prefix is what keeps this drop-in winning.
    assert _DROPIN.startswith("zz_")
