#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --input-format-parallel-parsing 1 --query "
    SELECT sum(cityHash64(*)) FROM file('$CUR_DIR/02951_data.jsonl.zst', JSONCompactEachRow, '
            time_offset Decimal64(3),
            lat Float64,
            lon Float64,
            altitude String,
            ground_speed Float32,
            track_degrees Float32,
            flags UInt32,
            vertical_rate Int32,
            aircraft Tuple(
                alert            Int64,
                alt_geom         Int64,
                gva              Int64,
                nac_p            Int64,
                nac_v            Int64,
                nic              Int64,
                nic_baro         Int64,
                rc               Int64,
                sda              Int64,
                sil              Int64,
                sil_type         String,
                spi              Int64,
                track            Float64,
                type             String,
                version          Int64,
                category         String,
                emergency        String,
                flight           String,
                squawk           String,
                baro_rate        Int64,
                nav_altitude_fms Int64,
                nav_altitude_mcp Int64,
                nav_modes        Array(String),
                nav_qnh          Float64,
                geom_rate        Int64,
                ias              Int64,
                mach             Float64,
                mag_heading      Float64,
                oat              Int64,
                roll             Float64,
                tas              Int64,
                tat              Int64,
                true_heading     Float64,
                wd               Int64,
                ws               Int64,
                track_rate       Float64,
                nav_heading      Float64
            ),
            source LowCardinality(String),
            geometric_altitude Int32,
            geometric_vertical_rate Int32,
            indicated_airspeed Int32,
            roll_angle Float32,
            hex String
        ')"
