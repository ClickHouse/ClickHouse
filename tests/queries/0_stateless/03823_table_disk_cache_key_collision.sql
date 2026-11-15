-- Tags: no-parallel, no-fasttest, no-replicated-database
-- Tag no-parallel - uses external data source
-- Tag no-fasttest - requires SSL for https
-- Tag no-replicated-database - because of https://github.com/ClickHouse/ClickHouse/issues/97287

-- Verifiy that two tables on separate web disks with table_disk=1 do not collide in mark caches
DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS store;

CREATE TABLE call_center(
      cc_call_center_sk         Int64 NOT NULL,
      cc_call_center_id         FixedString(16) NOT NULL,
      cc_rec_start_date         Date,
      cc_rec_end_date           Date,
      cc_closed_date_sk         UInt32,
      cc_open_date_sk           UInt32,
      cc_name                   String,
      cc_class                  String,
      cc_employees              Int64,
      cc_sq_ft                  Int64,
      cc_hours                  FixedString(20),
      cc_manager                String,
      cc_mkt_id                 Int64,
      cc_mkt_class              FixedString(50),
      cc_mkt_desc               String,
      cc_market_manager         String,
      cc_division               Int64,
      cc_division_name          String,
      cc_company                Int64,
      cc_company_name           FixedString(50),
      cc_street_number          FixedString(10),
      cc_street_name            String,
      cc_street_type            FixedString(15),
      cc_suite_number           FixedString(10),
      cc_city                   String,
      cc_county                 String,
      cc_state                  FixedString(2),
      cc_zip                    FixedString(10),
      cc_country                String,
      cc_gmt_offset             Decimal(5,2),
      cc_tax_percentage         Decimal(5,2),
      PRIMARY KEY (cc_call_center_sk)
)
SETTINGS
    data_type_default_nullable = 1,
    table_disk = 1,
    disk = disk(type = cache, path = 'filesystem_caches/stateful/call_center/', max_size = '4G',
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.us-east-1.amazonaws.com/call_center/'));

CREATE TABLE store (
    s_store_sk                Int64 NOT NULL,
    s_store_id                FixedString(16) NOT NULL,
    s_rec_start_date          Date,
    s_rec_end_date            Date,
    s_closed_date_sk          UInt32,
    s_store_name              String,
    s_number_employees        Int64,
    s_floor_space             Int64,
    s_hours                   FixedString(20),
    s_manager                 String,
    s_market_id               Int64,
    s_geography_class         String,
    s_market_desc             String,
    s_market_manager          String,
    s_division_id             Int64,
    s_division_name           String,
    s_company_id              Int64,
    s_company_name            String,
    s_street_number           String,
    s_street_name             String,
    s_street_type             FixedString(15),
    s_suite_number            FixedString(10),
    s_city                    String,
    s_county                  String,
    s_state                   FixedString(2),
    s_zip                     FixedString(10),
    s_country                 String,
    s_gmt_offset              Decimal(5,2),
    s_tax_percentage          Decimal(5,2),
    PRIMARY KEY (s_store_sk)
)
SETTINGS
    data_type_default_nullable = 1,
    table_disk = 1,
    disk = disk(type = cache, path = 'filesystem_caches/stateful/store/', max_size = '4G',
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.us-east-1.amazonaws.com/store/'));

SYSTEM DROP MARK CACHE;
SYSTEM DROP FILESYSTEM CACHE;

SELECT s_store_id FROM store LIMIT 1;
SELECT cc_call_center_id FROM call_center LIMIT 1;

DROP TABLE call_center;
DROP TABLE store;
