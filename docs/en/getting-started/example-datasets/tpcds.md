---
slug: /en/getting-started/example-datasets/tpcds
sidebar_label: TPC-DS
description:  "The TPC-DS benchmark data set and queries."
---

# TPC-DS (2012)

Similar to the [Star Schema Benchmark (SSB)](star-schema.md), TPC-DS is based on [TPC-H](tpch.md), but it took the opposite route, i.e. it expanded the number of joins needed by storing the data in a complex snowflake schema (24 instead of 8 tables).
The data distribution is skewed (e.g. normal and Poisson distributions).
It includes 99 reporting and ad-hoc queries with random substitutions.

References
- [The Making of TPC-DS](https://dl.acm.org/doi/10.5555/1182635.1164217) (Nambiar), 2006

First, checkout the TPC-DS repository and compile the data generator:

``` bash
git clone https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
make
```

Then, generate the data. Parameter `-scale` specifies the scale factor.

``` bash
./dsdgen -scale 1
```

Then, generate the queries (use the same scale factor):

```bash
./dsqgen -DIRECTORY ../query_templates/ -INPUT ../query_templates/templates.lst  -SCALE 1 # generates 99 queries in out/query_0.sql
```

Now create tables in ClickHouse.
You can either use the original table definitions in tools/tpcds.sql or "tuned" table definitions with properly defined primary key indexes and LowCardinality-type column types where it makes sense.

```sql
CREATE TABLE call_center(
      cc_call_center_sk         Int64,
      cc_call_center_id         LowCardinality(String),
      cc_rec_start_date         Nullable(Date),
      cc_rec_end_date           Nullable(Date),
      cc_closed_date_sk         Nullable(UInt32),
      cc_open_date_sk           Nullable(UInt32),
      cc_name                   LowCardinality(String),
      cc_class                  LowCardinality(String),
      cc_employees              Int32,
      cc_sq_ft                  Int32,
      cc_hours                  LowCardinality(String),
      cc_manager                LowCardinality(String),
      cc_mkt_id                 Int32,
      cc_mkt_class              LowCardinality(String),
      cc_mkt_desc               LowCardinality(String),
      cc_market_manager         LowCardinality(String),
      cc_division               Int32,
      cc_division_name          LowCardinality(String),
      cc_company                Int32,
      cc_company_name           LowCardinality(String),
      cc_street_number          LowCardinality(String),
      cc_street_name            LowCardinality(String),
      cc_street_type            LowCardinality(String),
      cc_suite_number           LowCardinality(String),
      cc_city                   LowCardinality(String),
      cc_county                 LowCardinality(String),
      cc_state                  LowCardinality(String),
      cc_zip                    LowCardinality(String),
      cc_country                LowCardinality(String),
      cc_gmt_offset             Decimal(7,2),
      cc_tax_percentage         Decimal(7,2),
      PRIMARY KEY (cc_call_center_sk)
);

CREATE TABLE catalog_page(
      cp_catalog_page_sk        Int64,
      cp_catalog_page_id        LowCardinality(String),
      cp_start_date_sk          Nullable(UInt32),
      cp_end_date_sk            Nullable(UInt32),
      cp_department             LowCardinality(Nullable(String)),
      cp_catalog_number         Nullable(Int32),
      cp_catalog_page_number    Nullable(Int32),
      cp_description            LowCardinality(Nullable(String)),
      cp_type                   LowCardinality(Nullable(String)),
      PRIMARY KEY (cp_catalog_page_sk)
);

CREATE TABLE catalog_returns(
    cr_returned_date_sk       Int32,
    cr_returned_time_sk       Int64,
    cr_item_sk                Int64,
    cr_refunded_customer_sk   Nullable(Int64),
    cr_refunded_cdemo_sk      Nullable(Int64),
    cr_refunded_hdemo_sk      Nullable(Int64),
    cr_refunded_addr_sk       Nullable(Int64),
    cr_returning_customer_sk  Nullable(Int64),
    cr_returning_cdemo_sk     Nullable(Int64),
    cr_returning_hdemo_sk     Nullable(Int64),
    cr_returning_addr_sk      Nullable(Int64),
    cr_call_center_sk         Nullable(Int64),
    cr_catalog_page_sk        Nullable(Int64),
    cr_ship_mode_sk           Nullable(Int64),
    cr_warehouse_sk           Nullable(Int64),
    cr_reason_sk              Nullable(Int64),
    cr_order_number           Int64,
    cr_return_quantity        Nullable(Int32),
    cr_return_amount          Nullable(Decimal(7,2)),
    cr_return_tax             Nullable(Decimal(7,2)),
    cr_return_amt_inc_tax     Nullable(Decimal(7,2)),
    cr_fee                    Nullable(Decimal(7,2)),
    cr_return_ship_cost       Nullable(Decimal(7,2)),
    cr_refunded_cash          Nullable(Decimal(7,2)),
    cr_reversed_charge        Nullable(Decimal(7,2)),
    cr_store_credit           Nullable(Decimal(7,2)),
    cr_net_loss               Nullable(Decimal(7,2)),
    PRIMARY KEY (cr_item_sk, cr_order_number)
);

CREATE TABLE catalog_sales (
    cs_sold_date_sk           Nullable(UInt32),
    cs_sold_time_sk           Nullable(Int64),
    cs_ship_date_sk           Nullable(UInt32),
    cs_bill_customer_sk       Nullable(Int64),
    cs_bill_cdemo_sk          Nullable(Int64),
    cs_bill_hdemo_sk          Nullable(Int64),
    cs_bill_addr_sk           Nullable(Int64),
    cs_ship_customer_sk       Nullable(Int64),
    cs_ship_cdemo_sk          Nullable(Int64),
    cs_ship_hdemo_sk          Nullable(Int64),
    cs_ship_addr_sk           Nullable(Int64),
    cs_call_center_sk         Nullable(Int64),
    cs_catalog_page_sk        Nullable(Int64),
    cs_ship_mode_sk           Nullable(Int64),
    cs_warehouse_sk           Nullable(Int64),
    cs_item_sk                Int64,
    cs_promo_sk               Nullable(Int64),
    cs_order_number           Int64,
    cs_quantity               Nullable(Int32),
    cs_wholesale_cost         Nullable(Decimal(7,2)),
    cs_list_price             Nullable(Decimal(7,2)),
    cs_sales_price            Nullable(Decimal(7,2)),
    cs_ext_discount_amt       Nullable(Decimal(7,2)),
    cs_ext_sales_price        Nullable(Decimal(7,2)),
    cs_ext_wholesale_cost     Nullable(Decimal(7,2)),
    cs_ext_list_price         Nullable(Decimal(7,2)),
    cs_ext_tax                Nullable(Decimal(7,2)),
    cs_coupon_amt             Nullable(Decimal(7,2)),
    cs_ext_ship_cost          Nullable(Decimal(7,2)),
    cs_net_paid               Nullable(Decimal(7,2)),
    cs_net_paid_inc_tax       Nullable(Decimal(7,2)),
    cs_net_paid_inc_ship      Nullable(Decimal(7,2)),
    cs_net_paid_inc_ship_tax  Nullable(Decimal(7,2)),
    cs_net_profit             Decimal(7,2),
    PRIMARY KEY (cs_item_sk, cs_order_number)
);

CREATE TABLE customer_address (
    ca_address_sk             Int64,
    ca_address_id             LowCardinality(String),
    ca_street_number          LowCardinality(Nullable(String)),
    ca_street_name            LowCardinality(Nullable(String)),
    ca_street_type            LowCardinality(Nullable(String)),
    ca_suite_number           LowCardinality(Nullable(String)),
    ca_city                   LowCardinality(Nullable(String)),
    ca_county                 LowCardinality(Nullable(String)),
    ca_state                  LowCardinality(Nullable(String)),
    ca_zip                    LowCardinality(Nullable(String)),
    ca_country                LowCardinality(Nullable(String)),
    ca_gmt_offset             Nullable(Decimal(7,2)),
    ca_location_type          LowCardinality(Nullable(String)),
    PRIMARY KEY (ca_address_sk)
);

CREATE TABLE customer_demographics (
    cd_demo_sk                Int64,
    cd_gender                 LowCardinality(String),
    cd_marital_status         LowCardinality(String),
    cd_education_status       LowCardinality(String),
    cd_purchase_estimate      Int32,
    cd_credit_rating          LowCardinality(String),
    cd_dep_count              Int32,
    cd_dep_employed_count     Int32,
    cd_dep_college_count      Int32,
    PRIMARY KEY (cd_demo_sk)
);

CREATE TABLE customer (
    c_customer_sk             Int64,
    c_customer_id             LowCardinality(String),
    c_current_cdemo_sk        Nullable(Int64),
    c_current_hdemo_sk        Nullable(Int64),
    c_current_addr_sk         Nullable(Int64),
    c_first_shipto_date_sk    Nullable(UInt32),
    c_first_sales_date_sk     Nullable(UInt32),
    c_salutation              LowCardinality(Nullable(String)),
    c_first_name              LowCardinality(Nullable(String)),
    c_last_name               LowCardinality(Nullable(String)),
    c_preferred_cust_flag     LowCardinality(Nullable(String)),
    c_birth_day               Nullable(Int32),
    c_birth_month             Nullable(Int32),
    c_birth_year              Nullable(Int32),
    c_birth_country           LowCardinality(Nullable(String)),
    c_login                   LowCardinality(Nullable(String)),
    c_email_address           LowCardinality(Nullable(String)),
    c_last_review_date        LowCardinality(Nullable(String)),
    PRIMARY KEY (c_customer_sk)
);

CREATE TABLE date_dim (
    d_date_sk                 UInt32,
    d_date_id                 LowCardinality(String),
    d_date                    Date,
    d_month_seq               UInt16,
    d_week_seq                UInt16,
    d_quarter_seq             UInt16,
    d_year                    UInt16,
    d_dow                     UInt16,
    d_moy                     UInt16,
    d_dom                     UInt16,
    d_qoy                     UInt16,
    d_fy_year                 UInt16,
    d_fy_quarter_seq          UInt16,
    d_fy_week_seq             UInt16,
    d_day_name                LowCardinality(String),
    d_quarter_name            LowCardinality(String),
    d_holiday                 LowCardinality(String),
    d_weekend                 LowCardinality(String),
    d_following_holiday       LowCardinality(String),
    d_first_dom               Int32,
    d_last_dom                Int32,
    d_same_day_ly             Int32,
    d_same_day_lq             Int32,
    d_current_day             LowCardinality(String),
    d_current_week            LowCardinality(String),
    d_current_month           LowCardinality(String),
    d_current_quarter         LowCardinality(String),
    d_current_year            LowCardinality(String),
    PRIMARY KEY (d_date_sk)
);

CREATE TABLE household_demographics (
    hd_demo_sk                Int64,
    hd_income_band_sk         Int64,
    hd_buy_potential          LowCardinality(String),
    hd_dep_count              Int32,
    hd_vehicle_count          Int32,
    PRIMARY KEY (hd_demo_sk)
);

CREATE TABLE income_band(
    ib_income_band_sk         Int64,
    ib_lower_bound            Int32,
    ib_upper_bound            Int32,
    PRIMARY KEY (ib_income_band_sk),
);

CREATE TABLE inventory (
    inv_date_sk             UInt32,
    inv_item_sk             Int64,
    inv_warehouse_sk        Int64,
    inv_quantity_on_hand    Nullable(Int32)
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk),
);

CREATE TABLE item (
    i_item_sk                 Int64,
    i_item_id                 LowCardinality(String),
    i_rec_start_date          LowCardinality(Nullable(String)),
    i_rec_end_date            LowCardinality(Nullable(String)),
    i_item_desc               LowCardinality(Nullable(String)),
    i_current_price           Nullable(Decimal(7,2)),
    i_wholesale_cost          Nullable(Decimal(7,2)),
    i_brand_id                Nullable(Int32),
    i_brand                   LowCardinality(Nullable(String)),
    i_class_id                Nullable(Int32),
    i_class                   LowCardinality(Nullable(String)),
    i_category_id             Nullable(Int32),
    i_category                LowCardinality(Nullable(String)),
    i_manufact_id             Nullable(Int32),
    i_manufact                LowCardinality(Nullable(String)),
    i_size                    LowCardinality(Nullable(String)),
    i_formulation             LowCardinality(Nullable(String)),
    i_color                   LowCardinality(Nullable(String)),
    i_units                   LowCardinality(Nullable(String)),
    i_container               LowCardinality(Nullable(String)),
    i_manager_id              Nullable(Int32),
    i_product_name            LowCardinality(Nullable(String)),
    PRIMARY KEY (i_item_sk)
);

CREATE TABLE promotion (
    p_promo_sk                Int64,
    p_promo_id                LowCardinality(String),
    p_start_date_sk           Nullable(UInt32),
    p_end_date_sk             Nullable(UInt32),
    p_item_sk                 Nullable(Int64),
    p_cost                    Nullable(Decimal(15,2)),
    p_response_target         Nullable(Int32),
    p_promo_name              LowCardinality(Nullable(String)),
    p_channel_dmail           LowCardinality(Nullable(String)),
    p_channel_email           LowCardinality(Nullable(String)),
    p_channel_catalog         LowCardinality(Nullable(String)),
    p_channel_tv              LowCardinality(Nullable(String)),
    p_channel_radio           LowCardinality(Nullable(String)),
    p_channel_press           LowCardinality(Nullable(String)),
    p_channel_event           LowCardinality(Nullable(String)),
    p_channel_demo            LowCardinality(Nullable(String)),
    p_channel_details         LowCardinality(Nullable(String)),
    p_purpose                 LowCardinality(Nullable(String)),
    p_discount_active         LowCardinality(Nullable(String)),
    PRIMARY KEY (p_promo_sk)
);

CREATE TABLE reason(
      r_reason_sk               Int64,
      r_reason_id               LowCardinality(String),
      r_reason_desc             LowCardinality(String),
      PRIMARY KEY (r_reason_sk)
);

CREATE TABLE ship_mode(
      sm_ship_mode_sk           Int64,
      sm_ship_mode_id           LowCardinality(String),
      sm_type                   LowCardinality(String),
      sm_code                   LowCardinality(String),
      sm_carrier                LowCardinality(String),
      sm_contract               LowCardinality(String),
      PRIMARY KEY (sm_ship_mode_sk)
);

CREATE TABLE store_returns (
    sr_returned_date_sk       Nullable(UInt32),
    sr_return_time_sk         Nullable(Int64),
    sr_item_sk                Int64,
    sr_customer_sk            Nullable(Int64),
    sr_cdemo_sk               Nullable(Int64),
    sr_hdemo_sk               Nullable(Int64),
    sr_addr_sk                Nullable(Int64),
    sr_store_sk               Nullable(Int64),
    sr_reason_sk              Nullable(Int64),
    sr_ticket_number          Int64,
    sr_return_quantity        Nullable(Int32),
    sr_return_amt             Nullable(Decimal(7,2)),
    sr_return_tax             Nullable(Decimal(7,2)),
    sr_return_amt_inc_tax     Nullable(Decimal(7,2)),
    sr_fee                    Nullable(Decimal(7,2)),
    sr_return_ship_cost       Nullable(Decimal(7,2)),
    sr_refunded_cash          Nullable(Decimal(7,2)),
    sr_reversed_charge        Nullable(Decimal(7,2)),
    sr_store_credit           Nullable(Decimal(7,2)),
    sr_net_loss               Nullable(Decimal(7,2)),
    PRIMARY KEY (sr_item_sk, sr_ticket_number)
);

CREATE TABLE store_sales (
    ss_sold_date_sk           Nullable(UInt32),
    ss_sold_time_sk           Nullable(Int64),
    ss_item_sk                Int64,
    ss_customer_sk            Nullable(Int64),
    ss_cdemo_sk               Nullable(Int64),
    ss_hdemo_sk               Nullable(Int64),
    ss_addr_sk                Nullable(Int64),
    ss_store_sk               Nullable(Int64),
    ss_promo_sk               Nullable(Int64),
    ss_ticket_number          Int64,
    ss_quantity               Nullable(Int32),
    ss_wholesale_cost         Nullable(Decimal(7,2)),
    ss_list_price             Nullable(Decimal(7,2)),
    ss_sales_price            Nullable(Decimal(7,2)),
    ss_ext_discount_amt       Nullable(Decimal(7,2)),
    ss_ext_sales_price        Nullable(Decimal(7,2)),
    ss_ext_wholesale_cost     Nullable(Decimal(7,2)),
    ss_ext_list_price         Nullable(Decimal(7,2)),
    ss_ext_tax                Nullable(Decimal(7,2)),
    ss_coupon_amt             Nullable(Decimal(7,2)),
    ss_net_paid               Nullable(Decimal(7,2)),
    ss_net_paid_inc_tax       Nullable(Decimal(7,2)),
    ss_net_profit             Nullable(Decimal(7,2)),
    PRIMARY KEY (ss_item_sk, ss_ticket_number)
);

CREATE TABLE store (
    s_store_sk                Int64,
    s_store_id                LowCardinality(String),
    s_rec_start_date          LowCardinality(Nullable(String)),
    s_rec_end_date            LowCardinality(Nullable(String)),
    s_closed_date_sk          Nullable(UInt32),
    s_store_name              LowCardinality(Nullable(String)),
    s_number_employees        Nullable(Int32),
    s_floor_space             Nullable(Int32),
    s_hours                   LowCardinality(Nullable(String)),
    s_manager                 LowCardinality(Nullable(String)),
    s_market_id               Nullable(Int32),
    s_geography_class         LowCardinality(Nullable(String)),
    s_market_desc             LowCardinality(Nullable(String)),
    s_market_manager          LowCardinality(Nullable(String)),
    s_division_id             Nullable(Int32),
    s_division_name           LowCardinality(Nullable(String)),
    s_company_id              Nullable(Int32),
    s_company_name            LowCardinality(Nullable(String)),
    s_street_number           LowCardinality(Nullable(String)),
    s_street_name             LowCardinality(Nullable(String)),
    s_street_type             LowCardinality(Nullable(String)),
    s_suite_number            LowCardinality(Nullable(String)),
    s_city                    LowCardinality(Nullable(String)),
    s_county                  LowCardinality(Nullable(String)),
    s_state                   LowCardinality(Nullable(String)),
    s_zip                     LowCardinality(Nullable(String)),
    s_country                 LowCardinality(Nullable(String)),
    s_gmt_offset              Nullable(Decimal(7,2)),
    s_tax_precentage          Nullable(Decimal(7,2)),
    PRIMARY KEY (s_store_sk)
);

CREATE TABLE time_dim (
    t_time_sk                 UInt32,
    t_time_id                 LowCardinality(String),
    t_time                    UInt32,
    t_hour                    UInt8,
    t_minute                  UInt8,
    t_second                  UInt8,
    t_am_pm                   LowCardinality(String),
    t_shift                   LowCardinality(String),
    t_sub_shift               LowCardinality(String),
    t_meal_time               LowCardinality(Nullable(String)),
    PRIMARY KEY (t_time_sk)
);

CREATE TABLE warehouse(
      w_warehouse_sk            Int64,
      w_warehouse_id            LowCardinality(String),
      w_warehouse_name          LowCardinality(Nullable(String)),
      w_warehouse_sq_ft         Nullable(Int32),
      w_street_number           LowCardinality(Nullable(String)),
      w_street_name             LowCardinality(Nullable(String)),
      w_street_type             LowCardinality(Nullable(String)),
      w_suite_number            LowCardinality(Nullable(String)),
      w_city                    LowCardinality(Nullable(String)),
      w_county                  LowCardinality(Nullable(String)),
      w_state                   LowCardinality(Nullable(String)),
      w_zip                     LowCardinality(Nullable(String)),
      w_country                 LowCardinality(Nullable(String)),
      w_gmt_offset              Decimal(7,2),
      PRIMARY KEY (w_warehouse_sk)
);

CREATE TABLE web_page(
      wp_web_page_sk            Int64,
      wp_web_page_id            LowCardinality(String),
      wp_rec_start_date         LowCardinality(Nullable(String)),
      wp_rec_end_date           LowCardinality(Nullable(String)),
      wp_creation_date_sk       Nullable(UInt32),
      wp_access_date_sk         Nullable(UInt32),
      wp_autogen_flag           LowCardinality(Nullable(String)),
      wp_customer_sk            Nullable(Int64),
      wp_url                    LowCardinality(Nullable(String)),
      wp_type                   LowCardinality(Nullable(String)),
      wp_char_count             Nullable(Int32),
      wp_link_count             Nullable(Int32),
      wp_image_count            Nullable(Int32),
      wp_max_ad_count           Nullable(Int32),
      PRIMARY KEY (wp_web_page_sk)
);

CREATE TABLE web_returns (
    wr_returned_date_sk       Nullable(UInt32),
    wr_returned_time_sk       Nullable(Int64),
    wr_item_sk                Int64,
    wr_refunded_customer_sk   Nullable(Int64),
    wr_refunded_cdemo_sk      Nullable(Int64),
    wr_refunded_hdemo_sk      Nullable(Int64),
    wr_refunded_addr_sk       Nullable(Int64),
    wr_returning_customer_sk  Nullable(Int64),
    wr_returning_cdemo_sk     Nullable(Int64),
    wr_returning_hdemo_sk     Nullable(Int64),
    wr_returning_addr_sk      Nullable(Int64),
    wr_web_page_sk            Nullable(Int64),
    wr_reason_sk              Nullable(Int64),
    wr_order_number           Int64,
    wr_return_quantity        Nullable(Int32),
    wr_return_amt             Nullable(Decimal(7,2)),
    wr_return_tax             Nullable(Decimal(7,2)),
    wr_return_amt_inc_tax     Nullable(Decimal(7,2)),
    wr_fee                    Nullable(Decimal(7,2)),
    wr_return_ship_cost       Nullable(Decimal(7,2)),
    wr_refunded_cash          Nullable(Decimal(7,2)),
    wr_reversed_charge        Nullable(Decimal(7,2)),
    wr_account_credit         Nullable(Decimal(7,2)),
    wr_net_loss               Nullable(Decimal(7,2)),
    PRIMARY KEY (wr_item_sk, wr_order_number)
);

CREATE TABLE web_sales (
    ws_sold_date_sk           Nullable(UInt32),
    ws_sold_time_sk           Nullable(Int64),
    ws_ship_date_sk           Nullable(UInt32),
    ws_item_sk                Int64,
    ws_bill_customer_sk       Nullable(Int64),
    ws_bill_cdemo_sk          Nullable(Int64),
    ws_bill_hdemo_sk          Nullable(Int64),
    ws_bill_addr_sk           Nullable(Int64),
    ws_ship_customer_sk       Nullable(Int64),
    ws_ship_cdemo_sk          Nullable(Int64),
    ws_ship_hdemo_sk          Nullable(Int64),
    ws_ship_addr_sk           Nullable(Int64),
    ws_web_page_sk            Nullable(Int64),
    ws_web_site_sk            Nullable(Int64),
    ws_ship_mode_sk           Nullable(Int64),
    ws_warehouse_sk           Nullable(Int64),
    ws_promo_sk               Nullable(Int64),
    ws_order_number           Int64,
    ws_quantity               Nullable(Int32),
    ws_wholesale_cost         Nullable(Decimal(7,2)),
    ws_list_price             Nullable(Decimal(7,2)),
    ws_sales_price            Nullable(Decimal(7,2)),
    ws_ext_discount_amt       Nullable(Decimal(7,2)),
    ws_ext_sales_price        Nullable(Decimal(7,2)),
    ws_ext_wholesale_cost     Nullable(Decimal(7,2)),
    ws_ext_list_price         Nullable(Decimal(7,2)),
    ws_ext_tax                Nullable(Decimal(7,2)),
    ws_coupon_amt             Nullable(Decimal(7,2)),
    ws_ext_ship_cost          Nullable(Decimal(7,2)),
    ws_net_paid               Nullable(Decimal(7,2)),
    ws_net_paid_inc_tax       Nullable(Decimal(7,2)),
    ws_net_paid_inc_ship      Decimal(7,2),
    ws_net_paid_inc_ship_tax  Decimal(7,2),
    ws_net_profit             Decimal(7,2),
    PRIMARY KEY (ws_item_sk, ws_order_number)
);

CREATE TABLE web_site (
    web_site_sk           Int64,
    web_site_id           LowCardinality(String),
    web_rec_start_date    LowCardinality(String),
    web_rec_end_date      LowCardinality(Nullable(String)),
    web_name              LowCardinality(String),
    web_open_date_sk      UInt32,
    web_close_date_sk     Nullable(UInt32),
    web_class             LowCardinality(String),
    web_manager           LowCardinality(String),
    web_mkt_id            Int32,
    web_mkt_class         LowCardinality(String),
    web_mkt_desc          LowCardinality(String),
    web_market_manager    LowCardinality(String),
    web_company_id        Int32,
    web_company_name      LowCardinality(String),
    web_street_number     LowCardinality(String),
    web_street_name       LowCardinality(String),
    web_street_type       LowCardinality(String),
    web_suite_number      LowCardinality(String),
    web_city              LowCardinality(String),
    web_county            LowCardinality(String),
    web_state             LowCardinality(String),
    web_zip               LowCardinality(String),
    web_country           LowCardinality(String),
    web_gmt_offset        Decimal(7,2),
    web_tax_percentage    Decimal(7,2),
    PRIMARY KEY (web_site_sk)
);
```

The data can be imported as follows:

``` bash
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO call_center FORMAT CSV" < call_center.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO catalog_page FORMAT CSV" < catalog_page.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO catalog_returns FORMAT CSV" < catalog_returns.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO catalog_sales FORMAT CSV" < catalog_sales.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO customer FORMAT CSV" < customer.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO customer_address FORMAT CSV" < customer_address.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO customer_demographics FORMAT CSV" < customer_demographics.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO date_dim FORMAT CSV" < date_dim.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO household_demographics FORMAT CSV" < household_demographics.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO income_band FORMAT CSV" < income_band.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO inventory FORMAT CSV" < inventory.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO item FORMAT CSV" < item.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO promotion FORMAT CSV" < promotion.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO reason FORMAT CSV" < reason.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO ship_mode FORMAT CSV" < ship_mode.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO store FORMAT CSV" < store.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO store_returns FORMAT CSV" < store_returns.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO store_sales FORMAT CSV" < store_sales.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO time_dim FORMAT CSV" < time_dim.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO warehouse FORMAT CSV" < warehouse.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO web_page FORMAT CSV" < web_page.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO web_returns FORMAT CSV" < web_returns.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO web_sales FORMAT CSV" < web_sales.tbl
clickhouse-client --format_csv_delimiter '|' --query "INSERT INTO web_site FORMAT CSV" < web_site.tbl
```

Then run the generated queries.

::::warning
TPC-DS makes heavy use of correlated subqueries which are at the time of writing (September 2024) not supported by ClickHouse ([issue #6697](https://github.com/ClickHouse/ClickHouse/issues/6697)).
As a result, many of above benchmark queries will fail with errors.
::::
