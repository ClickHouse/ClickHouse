#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PNG format requires base64, and the fast-test build does not enable base64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04665_png_animation"
mkdir -p "${OUT}"

# Render over HTTP so the image is produced by the server. With the native protocol the result is
# formatted on the client, which would not exercise the server-side PNG encoder at all.
png_http()
{
    # $1: extra URL settings (image dimensions etc., may be empty); $2: query
    curl -sS "${CLICKHOUSE_URL}&$1" --data-binary "$2"
}

# Parse an APNG datastream and print its animation structure: the `acTL` declaration, the `fcTL` frame
# control chunks, and, on request, decoded pixels of individual frames. Every chunk CRC is verified, and
# the `fcTL`/`fdAT` sequence numbers are checked to be consecutive from zero, so a malformed datastream is
# reported here instead of silently passing.
PARSER="${OUT}/parse_apng.py"
cat > "${PARSER}" <<'PYEOF'
import sys, zlib, struct

def parse(path):
    data = open(path, "rb").read()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "bad signature"
    pos, chunks = 8, []
    while pos < len(data):
        length = struct.unpack(">I", data[pos:pos + 4])[0]
        ctype = data[pos + 4:pos + 8]
        payload = data[pos + 8:pos + 8 + length]
        crc = struct.unpack(">I", data[pos + 8 + length:pos + 12 + length])[0]
        if crc != (zlib.crc32(ctype + payload) & 0xffffffff):
            print("BAD CRC in " + ctype.decode())
        chunks.append((ctype.decode(), payload))
        pos += 12 + length
        if ctype == b"IEND":
            break
    return chunks

def unfilter(raw, width, height, channels):
    stride, prev, out, p = width * channels, bytearray(width * channels), bytearray(), 0
    for _ in range(height):
        f = raw[p]; p += 1
        line = bytearray(raw[p:p + stride]); p += stride
        for i in range(stride):
            a = line[i - channels] if i >= channels else 0
            b = prev[i]
            c = prev[i - channels] if i >= channels else 0
            if f == 1: line[i] = (line[i] + a) & 0xff
            elif f == 2: line[i] = (line[i] + b) & 0xff
            elif f == 3: line[i] = (line[i] + ((a + b) >> 1)) & 0xff
            elif f == 4:
                pp = a + b - c
                pa, pb, pc = abs(pp - a), abs(pp - b), abs(pp - c)
                line[i] = (line[i] + (a if pa <= pb and pa <= pc else (b if pb <= pc else c))) & 0xff
        out += line
        prev = line
    return out

chunks = parse(sys.argv[1])
width = height = color_type = 0
actl = None
frames, sequence = [], []
for ctype, payload in chunks:
    if ctype == "IHDR":
        width, height, _bit_depth, color_type = struct.unpack(">IIBB", payload[:10])
    elif ctype == "acTL":
        actl = struct.unpack(">II", payload)
    elif ctype == "fcTL":
        seq, fw, fh, fx, fy, dnum, dden, dispose, blend = struct.unpack(">IIIIIHHBB", payload)
        sequence.append(seq)
        frames.append({"delay": (dnum, dden), "rect": (fw, fh, fx, fy),
                       "dispose": dispose, "blend": blend, "data": b""})
    elif ctype == "IDAT":
        if frames:
            frames[-1]["data"] += payload
    elif ctype == "fdAT":
        sequence.append(struct.unpack(">I", payload[:4])[0])
        frames[-1]["data"] += payload[4:]

channels = {0: 1, 2: 3, 6: 4}[color_type]
print("size=%dx%d color_type=%d" % (width, height, color_type))
print("acTL=" + ("absent" if actl is None else "num_frames=%d num_plays=%d" % actl))
print("real_frames=%d" % len(frames))
print("sequence_consecutive=%s" % (sequence == list(range(len(sequence)))))
for i, f in enumerate(frames):
    print("frame%d delay=%d/%d rect=%dx%d+%d+%d dispose=%d blend=%d"
          % (i, f["delay"][0], f["delay"][1], f["rect"][0], f["rect"][1],
             f["rect"][2], f["rect"][3], f["dispose"], f["blend"]))

# Remaining arguments are "frame,x,y" pixel probes.
for arg in sys.argv[2:]:
    fi, x, y = map(int, arg.split(","))
    px = unfilter(zlib.decompress(frames[fi]["data"]), width, height, channels)
    off = (y * width + x) * channels
    print("pixel[%d](%d,%d)=%s" % (fi, x, y, ",".join(str(v) for v in px[off:off + channels])))
PYEOF

DIMS="output_format_image_width=2&output_format_image_height=1"

# Three frames, one per value of `t`, buffered (the default). Each frame holds two grayscale pixels.
echo "--- buffered, three frames ---"
png_http "${DIMS}" "
    SELECT intDiv(number, 2) AS t, toUInt8(intDiv(number, 2) * 100 + number % 2) AS v
    FROM numbers(6) ORDER BY number FORMAT PNG
" > "${OUT}/buffered.png"
python3 "${PARSER}" "${OUT}/buffered.png" 0,0,0 0,1,0 1,0,0 1,1,0 2,0,0 2,1,0

# The same result written out frame by frame. The frame count is not known when `acTL` is written, so an
# upper bound is declared, while the frames themselves are identical to the buffered case.
echo "--- streaming, three frames ---"
png_http "${DIMS}&output_format_image_streaming_animation=1" "
    SELECT intDiv(number, 2) AS t, toUInt8(intDiv(number, 2) * 100 + number % 2) AS v
    FROM numbers(6) ORDER BY number FORMAT PNG
" > "${OUT}/streaming.png"
python3 "${PARSER}" "${OUT}/streaming.png" 0,0,0 1,0,0 2,0,0

# With a terminal protocol the whole datastream is buffered before it is sent, so even in the streaming
# mode the exact frame count is patched into `acTL` and the payload conforms to the specification.
echo "--- streaming, three frames, iterm ---"
png_http "${DIMS}&output_format_image_streaming_animation=1&output_format_image_terminal_mode=iterm" "
    SELECT intDiv(number, 2) AS t, toUInt8(intDiv(number, 2) * 100 + number % 2) AS v
    FROM numbers(6) ORDER BY number FORMAT PNG
" > "${OUT}/streaming_iterm"
python3 - "${OUT}/streaming_iterm" "${OUT}/streaming_iterm.png" <<'PYEOF'
import base64, sys
data = open(sys.argv[1], "rb").read()
payload = data[data.index(b":") + 1 : data.index(b"\a")]
open(sys.argv[2], "wb").write(base64.b64decode(payload))
PYEOF
python3 "${PARSER}" "${OUT}/streaming_iterm.png" 0,0,0 1,0,0 2,0,0

# Buffered mode accepts `t` in any order and emits the frames sorted by `t`.
echo "--- buffered, descending t is reordered ---"
png_http "${DIMS}" "
    SELECT intDiv(number, 2) AS t, toUInt8(intDiv(number, 2) * 100 + number % 2) AS v
    FROM numbers(6) ORDER BY t DESC, number FORMAT PNG
" > "${OUT}/reordered.png"
python3 "${PARSER}" "${OUT}/reordered.png" 0,0,0 1,0,0 2,0,0

# Streaming mode requires `t` to be non-decreasing.
echo "--- streaming, descending t is rejected ---"
png_http "${DIMS}&output_format_image_streaming_animation=1" "
    SELECT intDiv(number, 2) AS t, toUInt8(number) AS v
    FROM numbers(6) ORDER BY t DESC, number FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# A single value of `t` is a one-frame animation; there is no following `t`, so the frame gets one time unit.
echo "--- single frame ---"
png_http "${DIMS}" "
    SELECT 7 AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" > "${OUT}/single.png"
python3 "${PARSER}" "${OUT}/single.png" 0,0,0 0,1,0

# The time scale settings change the frame delay: a step of 250 units of 1/1000 s is 1/4 s.
echo "--- time scale 1/1000, step of 250 ---"
png_http "${DIMS}&output_format_image_time_multiplier_seconds=1&output_format_image_time_divisor_seconds=1000" "
    SELECT number * 250 AS t, toUInt8(number) AS v FROM numbers(3) FORMAT PNG
" > "${OUT}/scale.png"
python3 "${PARSER}" "${OUT}/scale.png"

# A multiplier greater than one scales the other way: a step of 2 units of 5 s is 10 s.
echo "--- time scale 5/1, step of 2 ---"
png_http "${DIMS}&output_format_image_time_multiplier_seconds=5&output_format_image_time_divisor_seconds=1" "
    SELECT number * 2 AS t, toUInt8(number) AS v FROM numbers(3) FORMAT PNG
" > "${OUT}/scale_up.png"
python3 "${PARSER}" "${OUT}/scale_up.png"

# The time scale is taken as a reduced fraction, so a multiplier over the 16-bit limit of the frame
# delay parts is fine as long as the fraction reduces: a unit of 100000/60 s is exactly 5000/3 s.
echo "--- time scale 100000/60 reduces to 5000/3 ---"
png_http "${DIMS}&output_format_image_time_multiplier_seconds=100000" "
    SELECT toUInt8(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" > "${OUT}/reduced_scale.png"
python3 "${PARSER}" "${OUT}/reduced_scale.png"

# `fcTL` stores the actual frame delay, not the base unit, so a unit whose reduced denominator is over
# the 16-bit limit is still fine when the observed gaps of `t` reduce it further (microsecond units with
# coarser frame steps): a step of 2 units of 1/100000 s is exactly 1/50000 s.
echo "--- time scale 1/100000, step of 2 reduces to 1/50000 ---"
png_http "${DIMS}&output_format_image_time_divisor_seconds=100000" "
    SELECT number * 2 AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" > "${OUT}/reduced_delay.png"
python3 "${PARSER}" "${OUT}/reduced_delay.png"

# Each frame is an independent image: a pixel painted in one frame is not carried into the next.
echo "--- frames are independent ---"
png_http "output_format_image_width=2&output_format_image_height=2" "
    SELECT * FROM VALUES('t Int32, x Int32, y Int32, r UInt8, g UInt8, b UInt8',
        (0, 0, 0, 10, 20, 30),
        (1, 1, 1, 40, 50, 60)
    ) FORMAT PNG
" > "${OUT}/independent.png"
python3 "${PARSER}" "${OUT}/independent.png" 0,0,0 0,1,1 1,0,0 1,1,1

# An empty result still produces a valid one-frame animation.
echo "--- empty result ---"
png_http "${DIMS}" "
    SELECT toUInt8(0) AS t, toUInt8(0) AS v FROM numbers(0) FORMAT PNG
" > "${OUT}/empty.png"
python3 "${PARSER}" "${OUT}/empty.png" 0,0,0

# In the streaming mode the only frame of a one-frame animation is handed over after the whole result has
# been seen, so the exact count is declared instead of the upper bound.
echo "--- streaming, single frame ---"
png_http "${DIMS}&output_format_image_streaming_animation=1" "
    SELECT 7 AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" > "${OUT}/streaming_single.png"
python3 "${PARSER}" "${OUT}/streaming_single.png" 0,0,0 0,1,0

echo "--- streaming, empty result ---"
png_http "${DIMS}&output_format_image_streaming_animation=1" "
    SELECT toUInt8(0) AS t, toUInt8(0) AS v FROM numbers(0) FORMAT PNG
" > "${OUT}/streaming_empty.png"
python3 "${PARSER}" "${OUT}/streaming_empty.png" 0,0,0

# `t` of an unsigned type covers the whole `UInt64` range; the values above the maximum of `Int64` must not
# wrap around into negative ones, which would reorder the frames and break the monotonicity check.
echo "--- t of UInt64 above the maximum of Int64 ---"
png_http "${DIMS}" "
    SELECT * FROM VALUES('t UInt64, v UInt8',
        (9223372036854775807, 1),
        (9223372036854775808, 2),
        (18446744073709551615, 3)
    ) ORDER BY t FORMAT PNG
" > "${OUT}/big_t.png"
python3 "${PARSER}" "${OUT}/big_t.png" 0,0,0 1,0,0 2,0,0

echo "--- t of UInt64 above the maximum of Int64, streaming ---"
png_http "${DIMS}&output_format_image_streaming_animation=1" "
    SELECT * FROM VALUES('t UInt64, v UInt8',
        (9223372036854775807, 1),
        (9223372036854775808, 2),
        (18446744073709551615, 3)
    ) ORDER BY t FORMAT PNG
" > "${OUT}/big_t_streaming.png"
python3 "${PARSER}" "${OUT}/big_t_streaming.png" 0,0,0 1,0,0 2,0,0

# The widest possible gap: the delay does not fit into the 16-bit parts of the frame control chunk and is clamped to the
# longest expressible one, instead of overflowing the scaling factor.
echo "--- the whole Int64 range in one gap ---"
png_http "${DIMS}&output_format_image_time_multiplier_seconds=1&output_format_image_time_divisor_seconds=1" "
    SELECT * FROM VALUES('t Int64, v UInt8',
        (-9223372036854775808, 1),
        (9223372036854775807, 2)
    ) ORDER BY t FORMAT PNG
" > "${OUT}/wide_gap.png"
python3 "${PARSER}" "${OUT}/wide_gap.png"

# A delay that does not fit into the 16-bit parts and whose scaling factor does not divide the
# denominator: the pair is derived with rounding, not by floor-dividing both parts. A gap of 65536 units
# of 1/3 s is 65536/3 s and comes out as 43691/2 = 21845.5 s, next to the exact 21845.33 s; a floor
# division of both parts by the factor 2 would have given 32768/1 s, half as long again.
echo "--- over-long delay is rounded, not floored ---"
png_http "${DIMS}&output_format_image_time_divisor_seconds=3" "
    SELECT * FROM VALUES('t UInt32, v UInt8',
        (0, 1),
        (65536, 2)
    ) ORDER BY t FORMAT PNG
" > "${OUT}/rounded_delay.png"
python3 "${PARSER}" "${OUT}/rounded_delay.png"

# Without a `t` column the output is a still image and carries no animation chunks.
echo "--- no t column, still image ---"
png_http "${DIMS}" "
    SELECT toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" > "${OUT}/still.png"
python3 "${PARSER}" "${OUT}/still.png"

echo "--- errors ---"
# Sixel cannot represent an animation.
png_http "${DIMS}&output_format_image_terminal_mode=sixel" "
    SELECT toUInt8(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# The Kitty graphics protocol animates with per-frame commands, not with an animated datastream, so it would
# display an `APNG` as a still image.
png_http "${DIMS}&output_format_image_terminal_mode=kitty" "
    SELECT toUInt8(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# The parts of the time scale must not be zero.
png_http "${DIMS}&output_format_image_time_divisor_seconds=0" "
    SELECT toUInt8(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# The reduced denominator of each actual frame delay must fit into the 16-bit part of the frame delay,
# and a step of 1 unit of 1/100000 s does not reduce.
png_http "${DIMS}&output_format_image_time_divisor_seconds=100000" "
    SELECT toUInt8(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# `t` must be an integer.
png_http "${DIMS}" "
    SELECT toFloat64(number) AS t, toUInt8(number) AS v FROM numbers(2) FORMAT PNG
" | grep -oE "BAD_ARGUMENTS" | head -1

# A frame cannot hold more rows than the image has pixels.
png_http "${DIMS}" "
    SELECT toUInt8(0) AS t, toUInt8(number) AS v FROM numbers(3) FORMAT PNG
" | grep -oE "TOO_MANY_ROWS" | head -1

rm -rf "${OUT}"
