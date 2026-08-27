#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Iceberg table function is not in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Iceberg table whose decimal column is `Decimal(38, 30)`, so the column bounds in its manifest
# hold an unscaled value 14 bytes wide. Decoding a bound used to run in `int64_t`, where the
# big-endian accumulation, the sign extension and the 10^scale scaler all overflow; that is undefined
# behaviour and it aborts a build with the undefined behaviour sanitizer. A bound that wide never
# becomes a `Field`, so the results below are the same before and after the fix: what they assert is
# that reading the manifest with a filter present is free of undefined behaviour.

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_decimal"

cleanup()
{
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

mkdir -p "${TABLE_PATH}"

# ClickHouse cannot write a decimal column to Iceberg, so the table cannot be built by the test.
# This one was produced by Spark and attached to
# https://github.com/ClickHouse/ClickHouse/issues/114929:
#
#   CREATE TABLE dec_min (id int, d decimal(38, 30)) USING iceberg
#     TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'Parquet');
#   INSERT INTO dec_min SELECT 1, decimal(42.42);
#
# repacked here without the Spark checksum files. Python is used so the fixture setup does not depend
# on platform specific base64 or tar command line options.
python3 - "${TABLE_PATH}" <<'PY'
import base64
import io
from pathlib import Path
import sys
import tarfile

table_path = Path(sys.argv[1])
archive = base64.b64decode("""
H4sIAHiigGoAA+1ce6wcVRnfuxa81raB2l1u9aqHja1t3NnOcx9NMW0EQqEt2FuwUpu9Z2dme4fu
ziw7s71cmmv4AwyaILYlBCuBGCAh+od/+EhETQpqjIFGLIkVAwkRYqJ/4IOqMcToOXPmcWZ2Znbn
du8t2j3pvZc9853vdc535ved7yylbQq04LZSncWNYZlaWVZYoVlm5FpVZsSKgLoUgWfKDVjly1yT
rwhlRIapuVIHdu/uqVYmuSHSsigC/LdSluy/qLl/WZYXBcBJvMhyPFvhOMDyPC8IGcAO4DuS1jMt
2EWqLMC5BZhANz+nqq2E50GjwIi1XL52Rf79mWwmsxfK4NYZcBA4DfdlPoB+ePRzN/rBn58ajuWu
Awf2O/+JRzyGfj4YIpnw+3Oy0S7BTqelljpd45iqQ11WMxNI3lv/eXtXblN7BEaOW1wrkfhfxugf
GP+cIFbC8Y/2hHH8r0S7bdd+LpfJTeZA7sS5M9+8YjqXtT9OZjJbvr/t35mDgAQr+kH9IFfNPf/n
H/30fX1k92Fm2fUf/exzn3p426R+Hxmy8ersxqlVeLzzJ5fdmM9szKPfWfLwqvCoiK7AoOn8ZG5f
HhNO5+/INfPk+Y6brrBgo6XmVqFPmzJTWU25PZvJrUMaZ6YmlE2rcztye3Krpr+A/6IhiNn0xuLm
DLJj46ZJJBQNyK3JZ/Pl/L7NkzuIsuR3PrOF+oTE5rAnspkdW4lGmfwb2dyG/I8nctszmOE6wnCC
8NuXb26+Y0e/Tf09SM4QVFHyL2D5/5jI3ZXJ3z+Rz26ezH9vYgOmnFqnyWpD7R4pmfKc2oZTX584
XrAWOmphe8G0uj3ZKhQL5BGjKYXtbLHQ1NSWYha2HzpewD1csaDDNqZHn4qFrnp3T+uq6AEarRZd
XppuFRaLZATvjUgYoKiy1oatLUK1CAR2a2Hx8GJm6mZnP2HaXXBM7ZqaoQOuxEklFmxp9LSWAsRy
WUK7hCKUVbFcRVtUVeD5ZqVa5jkFVmRe4WSeVxV168bidCaD/mU+kiVLfAUi6X+zlba1VQs6GLCm
Ck2OlWpMRSqj3V+VK0xVhg1GEmG52qxybKWpMm22BI91jRQy0uM/ThLH+//KtDH+u6wbFf/LFP2D
4p+TpDIfjn9B5MbxvxLt1sZdE1etIRjgB9lRoYMmbJnp4EFwRAQ+yONl5+CY+6qeol1VNrqYlcO1
DXWtqZpWXdWt7kIhoLJDgibc6pmFgHoOHbET6+rS6rBjzhlW3TaQDDhU0HutFvrYMvQjhcNFpG0T
9lpWYTvupzlxNCdkqop2trreayNItgRuAsWtqbXU+sWzFCmWeA+oY74en1gnd/lIx8qGbqm2MwOe
VQwZ/fdnyEMTGE1gzakAS9oO2Ouw2CLgrusYpmZhyKeoLdVSzSLgr0P2wZZmLbh9gWlCe0TYIR1o
zfnS0RrWkP2uAnsMGdoCbt+/G8xr1hy4cQbY60kN8mXZMN+m0W1DK5bzjYgGEBqAh20HeK0WgdGV
8S/g4NqQFHpxIArLtn4I33NswPuHF101bnOZAOxTYPXQK7VILISgAU1VAegZ9r0nDpgdVQ7pxVN6
EeF12ejR02ovK1fqPnvt4VklxCbQdG+CQ6z7V7B2r1rX9HpjgcxulIADhgVbNjeAyTF7l5zmTS8G
2Wj12rrN3eyLCs+zsNuFeIfQLLVtJnj8KMdV6sc4rhq56I+qC/FbCRpJqXUMtnpqn5U0dXURT2fL
OKLJsHWAkLVhB+1/rjf2wg72NbEQaAqwDPQPe8h2Dp58zTxaSNqU2GpYJzLBo3FVDbmKZ5fiqloa
V/HsRbjKNreIVpLc6ikokgE2F0BdAfvgvmTf0Urip/VRO5DnsAOjN9hkB/JcKgfyS3Kg7SmbOXB3
hXhvcfROinD9yJ0lVJGzhNoSnCX0BUGSs4TaEp3l7o1oXRGvedsjIUx0n0BvHi1jXu3WG8h1ymjW
WRmvs8pS1lk53nUROzNfWZLvbHuBbW+ik3iJ0qbX6YzWSXg3E5aym/EJu1mEk4SlbWe2vcM4iV7t
SPG6m/T1Y0ainCf1Bl3uLnRsrIDGAXccaLSMRvLiDYDfDoJxdaPZNNUUge8GIgJ/bQQdHb6C75IZ
zNY+9wU+7wSVaGjjYkuE7FNoROY5qFDZV+gGF7DKRhuBLM1EbrMVALuvH6AbvYxNo4u81VXQWo7K
O7ASvg8QLbBpkYxEESIbQHZNtYuTB6XeD/w9SQ7Q9YTdiLoXgG0kYqygIHUw9ZZ+UL0Vr1ZIsKiN
3qw5hI8hen04aB54OiSrLdih4ffgF9cGOxuUDZQorkNDW9BSpwkCZ5yj4yzvo2oGw1w0NstOBw/E
nyilzHhZb3PwssghzsT9PDmYT8alv/a6dwcL/uC+fG8oBqLHICZrHIoLlbhT6yXa+H6nhg8OBF8p
P2tM8ORIs0hvWlg26Bonf4zRI0U+6UvgghK8THI4GcNmlr48f5ronDL1NI0gq/R18ldwKJ+McUKK
/NIXEl7locxyGFGxmaYvpUot20COGRNDnovxu7yIX71uXmh/8Fa3jRDcJND56KtHngZNWEyZEfo2
1IoBXOLD8bQ21GJt4NlR2ZCcqnlGcX4kR2VlKS3juXjL+Iu3rC+H8swQKsWEdCmlFUI11gqhNgor
BiY3nl28VIzJY9LOTDl+Ziq+TQ1Nh/bpb2qr6LTDV98P+1CGkVb9+JAR2FGoTycE/qryX0Ih7B/3
5vcUGCIJ8MX4754w3h/kppZm9uNqwevoTwViXBKREPjq+WswhP2XpF2Z1i6YFsQol5wceHqKFMYM
5QFJBZbofMDnSr+EI6F/HPMwJFmmFACBevRvOojZVx06vM7Bh5OYSwa+/NcvPrj72ufkb79Unfzd
u7/JnsnKjee++8zxX7zz1D8/+WRDmZ397fPP1p84+MjpE/U7H//WkydPP3rw9Ez26VNvmsI7F2bW
37L+72t37jy/8/y5Yu3nZ6pn91RePVf4/PmfvTa15ZFG6+mjjz4yB5XmY01Fb1154Mr97Wc/97WD
Bw6evvOFa96Y2jD1pw+/YH31G19+4JcPb3po4qaJqx9szMLZjFDd+fbsCbj+5NrXnnn9O786cT7z
1puPzs42fp/5o5LpU/lSV/yCLXD/A2cnTIWvCkJFQptetcZVOLFa5hmOGaY4HFcZTn//QxTK7Lj+
uyJtfP/jsm5U/C9T9A+8/8Gi0A/HPyeWx/G/Eg3f/1hH3634w8cH362gS7uBM3GPYiSVeClQifd4
t1T9CM09fbVYii6+1zHkoE9cg8AKPUSQCvTwkQeC2vNdzQrrSx8wp7oN4RpHn2VFXXqQAgXluAsg
QZfMOFRuxoZWsh4QCuahCaCiqEpIFn0m3db02AsnQXl7UAqFmJohsVpQaEgUXVOyValHXb0JGeZQ
4FkhyBIPTJASuIBApOCVYoYvONAztcvmad8m8iqeNEv63oF6D8oU0EofzPUGhzKBMe18shSG0PZ6
QpjAtt/RXWO+j2fAz8QFmCyKI8dHeWAgU88DsXyFCAcMZOs6IJarGBX5IygWdiU2+oII3gegppt1
p6biptiG0VKh7m9cKFUEGkqX9AWyfc3hoCRnRf7JqltJpGe0Ftp0iDCo99nkyTw8SCjUI2TGlmgk
ji4vUgc8A6uL/uEyde6CD7ztJNFe7cmC6RcEdTSTQjB1YpJGMGcXpegjATT/VGWy125DFIGYowrl
OUCfxsezZendfUXqtBJXiy6n5d39F5FtjkCFfeW2j7kbPkM2/CwXKrZ96dqUxTaJqs+EkcXoajQS
VaPpxxgjqRtIUWUZCm0MUfiKRh+eAK4ymnpaEg7xhUlDlSTjHZYGkfhSy8VkLDKM5CGxiT9x/rFZ
FCoZytghUIovTywm4ZMhZrQfr/jMqRPQaKQyBP9I5OKL8CcpGrMMISEKw/irgC8moJdh5qMPzfi8
hX7/pGYfhWt8CWKfe1ILiEA4vvsr/fvMEk+4JVzr9E64B1b0JaqsGEY8MVZdDALyPVqNEAv1eJuH
FhuBgXypbFQ9a/iCyjC4xxfGRVWfliIsHusE0ExccSUe0/i61pa11HR48RNIKNaRQieTeIVkjr18
/19ee/wB/tPXXNj34r03z2Vfnjh0Jr4SYM7cst6uBjAzD72yZu0epnd279mTvb37T53tMSc/dMra
u3f92eK54pq1L72yv2euvbD+xa/87cm3eJblXn/13E/e/Nev3z11K8dP/HB2VZ/oS32mk6YFzv+P
cSX3U+ku09BHJCP1+T9XFqXx979Wpo3P/y/rRsX/MkX/wPjnWZENxz8vjON/Rdrx1QAUgql8AWwH
fBH32zcpmF4PvdpRX6HZrMGGAhWGg5zEiJJSZmCz2WTKrFqriFyl2oCNgj3QvRRgD9s2j17ZcwbK
XLc5xxDor1xHSZxDDE30Pg8eHuCBrP+010GrUlWYtokfIEnlSllka6woUkTkIgxDlCUGyL0uQQve
MYPHl3TZ/A4B7AVsL4YkWGUX5ZLu/tHYZwT/UsNRJyHhiu5nGwphhjhbdDs9JIQeECzkPvHk43TI
7lwshpnz/czT8Q5/t5bIAYdX279t3zjT5F6b9p0WvJ0RdF6YOOQjmr89XT4vMqhWqwWFG12LsS+y
BOfN6w6K76NMkI/eMwgIW5pqP3MZzOtk3RW6huHNvX3OUiIBUnJP0TDRbe6XOmk65z5uCd/w6aom
jibGPlWzh9xrWgp292JwbfpIFlMx9urBl3SIcg61S+VaQzotFGUo25QDvdQkRT73BLaMI3S/u/lT
/asXL/X+NG7L24L4n3/v4H9+/P5fkTbG/5d1o/H/8kT/wPgXBUHsw//iOP5XpL2H8T83EP/zPC+N
8f8Y//8/4f+IUnswISDy2lDT/Y80qk9kE1wKjS561845K8BTLJRquNMbE6ApRBcsra2i7bbdiQtk
zIxUGQK24dny9xOEFVSdWvMm8vpRjCBKzj6Fd58W4zKvVqUK71PbVUPGznNI6QOP4MLPne8aRj+0
xzG41G8/r4iCTyHPQf0IovFXNynRhRjZX26Lk0IeJkkhFPFGOM/tSiFFwYYp3K/LMm6NP5LK/bpM
HJWqH9F0lX59FISSVJIov3U6zDCT43DydjZ7av3Hzn2SgKRdHYi2Z7CbPAJcqVriwBYUgG3NAjVZ
ZZtyk4XNSkOVm7wElVpTrtXKstCACg+bitxUBEFxd8GiG1ykLs/YddHkd9ho709Hv3KorWtUKf/x
FBE5MLwp/foPEQZJcg/x6MH2BeNh/R4+ty4QdcZnF8O0YP5P4oqZQ/ijZKn3DP5few/V0uf/FZQV
jPH/irRx/n9ZNzr/X57oHxz/LMuF839JqozjfyUaf6kVGLdxG7dxG7dL0v4LwMIm5ABsAAA=
""")

with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as fixture:
    filter_options = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
    fixture.extractall(table_path, **filter_options)
PY

# Any filter makes the manifest bounds be parsed; it does not have to touch the decimal column.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${TABLE_PATH}') WHERE id > 0"
${CLICKHOUSE_CLIENT} --query "SELECT id, toString(d) FROM icebergLocal('${TABLE_PATH}') WHERE d > 0"
