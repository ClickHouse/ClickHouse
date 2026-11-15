--- 2.2.2.2 
--- The datatypes do not correspond to any specific SQL-standard datatype.
--- The definitions are provided to highlight the properties that are required for a particular column. 
--- The benchmark implementer may employ any internal representation or SQL datatype that meets those requirements.

--- Version with Nullable and LowCardinality wrappers:
--- https://pastila.nl/?00048602/c66434dc77924ca267bb442762e93cb3#VXcoOhh8OXk4Kg3qOvrseA==GCM

--- Types mapping:
---  - identifier: Int64, except *_date_sk and *_time_sk columns which use UInt32
---  - integer: Int64
---  - decimal(P,S): Decimal(P,S)
---  - char(N): FixedString(N)
---  - varchar(N): String
---  - date: Date

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
);

CREATE TABLE catalog_page(
      cp_catalog_page_sk        Int64 NOT NULL,
      cp_catalog_page_id        FixedString(16) NOT NULL,
      cp_start_date_sk          UInt32,
      cp_end_date_sk            UInt32,
      cp_department             String,
      cp_catalog_number         Int64,
      cp_catalog_page_number    Int64,
      cp_description            String,
      cp_type                   String,
      PRIMARY KEY (cp_catalog_page_sk)
);

CREATE TABLE catalog_returns(
    cr_returned_date_sk       UInt32,
    cr_returned_time_sk       UInt32,
    cr_item_sk                Int64 NOT NULL,
    cr_refunded_customer_sk   Int64,
    cr_refunded_cdemo_sk      Int64,
    cr_refunded_hdemo_sk      Int64,
    cr_refunded_addr_sk       Int64,
    cr_returning_customer_sk  Int64,
    cr_returning_cdemo_sk     Int64,
    cr_returning_hdemo_sk     Int64,
    cr_returning_addr_sk      Int64,
    cr_call_center_sk         Int64,
    cr_catalog_page_sk        Int64,
    cr_ship_mode_sk           Int64,
    cr_warehouse_sk           Int64,
    cr_reason_sk              Int64,
    cr_order_number           Int64 NOT NULL,
    cr_return_quantity        Int64,
    cr_return_amount          Decimal(7,2),
    cr_return_tax             Decimal(7,2),
    cr_return_amt_inc_tax     Decimal(7,2),
    cr_fee                    Decimal(7,2),
    cr_return_ship_cost       Decimal(7,2),
    cr_refunded_cash          Decimal(7,2),
    cr_reversed_charge        Decimal(7,2),
    cr_store_credit           Decimal(7,2),
    cr_net_loss               Decimal(7,2),
    PRIMARY KEY (cr_item_sk, cr_order_number)
);

CREATE TABLE catalog_sales (
    cs_sold_date_sk           UInt32,
    cs_sold_time_sk           UInt32,
    cs_ship_date_sk           UInt32,
    cs_bill_customer_sk       Int64,
    cs_bill_cdemo_sk          Int64,
    cs_bill_hdemo_sk          Int64,
    cs_bill_addr_sk           Int64,
    cs_ship_customer_sk       Int64,
    cs_ship_cdemo_sk          Int64,
    cs_ship_hdemo_sk          Int64,
    cs_ship_addr_sk           Int64,
    cs_call_center_sk         Int64,
    cs_catalog_page_sk        Int64,
    cs_ship_mode_sk           Int64,
    cs_warehouse_sk           Int64,
    cs_item_sk                Int64 NOT NULL,
    cs_promo_sk               Int64,
    cs_order_number           Int64 NOT NULL,
    cs_quantity               Int64,
    cs_wholesale_cost         Decimal(7,2),
    cs_list_price             Decimal(7,2),
    cs_sales_price            Decimal(7,2),
    cs_ext_discount_amt       Decimal(7,2),
    cs_ext_sales_price        Decimal(7,2),
    cs_ext_wholesale_cost     Decimal(7,2),
    cs_ext_list_price         Decimal(7,2),
    cs_ext_tax                Decimal(7,2),
    cs_coupon_amt             Decimal(7,2),
    cs_ext_ship_cost          Decimal(7,2),
    cs_net_paid               Decimal(7,2),
    cs_net_paid_inc_tax       Decimal(7,2),
    cs_net_paid_inc_ship      Decimal(7,2),
    cs_net_paid_inc_ship_tax  Decimal(7,2),
    cs_net_profit             Decimal(7,2),
    PRIMARY KEY (cs_item_sk, cs_order_number)
);

CREATE TABLE customer_address (
    ca_address_sk             Int64 NOT NULL,
    ca_address_id             FixedString(16) NOT NULL,
    ca_street_number          FixedString(10),
    ca_street_name            String,
    ca_street_type            FixedString(15),
    ca_suite_number           FixedString(10),
    ca_city                   String,
    ca_county                 String,
    ca_state                  FixedString(2),
    ca_zip                    FixedString(10),
    ca_country                String,
    ca_gmt_offset             Decimal(5,2),
    ca_location_type          FixedString(20),
    PRIMARY KEY (ca_address_sk)
);

CREATE TABLE customer_demographics (
    cd_demo_sk                Int64 NOT NULL,
    cd_gender                 FixedString(1),
    cd_marital_status         FixedString(1),
    cd_education_status       FixedString(20),
    cd_purchase_estimate      Int64,
    cd_credit_rating          FixedString(10),
    cd_dep_count              Int64,
    cd_dep_employed_count     Int64,
    cd_dep_college_count      Int64,
    PRIMARY KEY (cd_demo_sk)
);

CREATE TABLE customer (
    c_customer_sk             Int64 NOT NULL,
    c_customer_id             FixedString(16) NOT NULL,
    c_current_cdemo_sk        Int64,
    c_current_hdemo_sk        Int64,
    c_current_addr_sk         Int64,
    c_first_shipto_date_sk    UInt32,
    c_first_sales_date_sk     UInt32,
    c_salutation              FixedString(10),
    c_first_name              FixedString(20),
    c_last_name               FixedString(30),
    c_preferred_cust_flag     FixedString(1),
    c_birth_day               Int64,
    c_birth_month             Int64,
    c_birth_year              Int64,
    c_birth_country           String,
    c_login                   FixedString(13),
    c_email_address           FixedString(50),
    c_last_review_date_sk     UInt32,
    PRIMARY KEY (c_customer_sk)
);

CREATE TABLE date_dim (
    d_date_sk                 UInt32 NOT NULL,
    d_date_id                 FixedString(16) NOT NULL,
    d_date                    Date NOT NULL,
    d_month_seq               Int64,
    d_week_seq                Int64,
    d_quarter_seq             Int64,
    d_year                    Int64,
    d_dow                     Int64,
    d_moy                     Int64,
    d_dom                     Int64,
    d_qoy                     Int64,
    d_fy_year                 Int64,
    d_fy_quarter_seq          Int64,
    d_fy_week_seq             Int64,
    d_day_name                FixedString(9),
    d_quarter_name            FixedString(6),
    d_holiday                 FixedString(1),
    d_weekend                 FixedString(1),
    d_following_holiday       FixedString(1),
    d_first_dom               Int64,
    d_last_dom                Int64,
    d_same_day_ly             Int64,
    d_same_day_lq             Int64,
    d_current_day             FixedString(1),
    d_current_week            FixedString(1),
    d_current_month           FixedString(1),
    d_current_quarter         FixedString(1),
    d_current_year            FixedString(1),
    PRIMARY KEY (d_date_sk)
);

CREATE TABLE household_demographics (
    hd_demo_sk                Int64 NOT NULL,
    hd_income_band_sk         Int64,
    hd_buy_potential          FixedString(15),
    hd_dep_count              Int64,
    hd_vehicle_count          Int64,
    PRIMARY KEY (hd_demo_sk)
);

CREATE TABLE income_band(
    ib_income_band_sk         Int64 NOT NULL,
    ib_lower_bound            Int64,
    ib_upper_bound            Int64,
    PRIMARY KEY (ib_income_band_sk)
);

CREATE TABLE inventory (
    inv_date_sk             UInt32 NOT NULL,
    inv_item_sk             Int64 NOT NULL,
    inv_warehouse_sk        Int64 NOT NULL,
    inv_quantity_on_hand    Int64,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

CREATE TABLE item (
    i_item_sk                 Int64 NOT NULL,
    i_item_id                 FixedString(16) NOT NULL,
    i_rec_start_date          Date,
    i_rec_end_date            Date,
    i_item_desc               String,
    i_current_price           Decimal(7,2),
    i_wholesale_cost          Decimal(7,2),
    i_brand_id                Int64,
    i_brand                   FixedString(50),
    i_class_id                Int64,
    i_class                   FixedString(50),
    i_category_id             Int64,
    i_category                FixedString(50),
    i_manufact_id             Int64,
    i_manufact                FixedString(50),
    i_size                    FixedString(20),
    i_formulation             FixedString(20),
    i_color                   FixedString(20),
    i_units                   FixedString(10),
    i_container               FixedString(10),
    i_manager_id              Int64,
    i_product_name            FixedString(50),
    PRIMARY KEY (i_item_sk)
);

CREATE TABLE promotion (
    p_promo_sk                Int64 NOT NULL,
    p_promo_id                FixedString(16) NOT NULL,
    p_start_date_sk           UInt32,
    p_end_date_sk             UInt32,
    p_item_sk                 Int64,
    p_cost                    Decimal(15,2),
    p_response_target         Int64,
    p_promo_name              FixedString(50),
    p_channel_dmail           FixedString(1),
    p_channel_email           FixedString(1),
    p_channel_catalog         FixedString(1),
    p_channel_tv              FixedString(1),
    p_channel_radio           FixedString(1),
    p_channel_press           FixedString(1),
    p_channel_event           FixedString(1),
    p_channel_demo            FixedString(1),
    p_channel_details         String,
    p_purpose                 FixedString(15),
    p_discount_active         FixedString(1),
    PRIMARY KEY (p_promo_sk)
);

CREATE TABLE reason(
      r_reason_sk               Int64 NOT NULL,
      r_reason_id               FixedString(16) NOT NULL,
      r_reason_desc             FixedString(100),
      PRIMARY KEY (r_reason_sk)
);

CREATE TABLE ship_mode(
      sm_ship_mode_sk           Int64 NOT NULL,
      sm_ship_mode_id           FixedString(16) NOT NULL,
      sm_type                   FixedString(30),
      sm_code                   FixedString(10),
      sm_carrier                FixedString(20),
      sm_contract               FixedString(20),
      PRIMARY KEY (sm_ship_mode_sk)
);

CREATE TABLE store_returns (
    sr_returned_date_sk       UInt32,
    sr_return_time_sk         UInt32,
    sr_item_sk                Int64 NOT NULL,
    sr_customer_sk            Int64,
    sr_cdemo_sk               Int64,
    sr_hdemo_sk               Int64,
    sr_addr_sk                Int64,
    sr_store_sk               Int64,
    sr_reason_sk              Int64,
    sr_ticket_number          Int64 NOT NULL,
    sr_return_quantity        Int64,
    sr_return_amt             Decimal(7,2),
    sr_return_tax             Decimal(7,2),
    sr_return_amt_inc_tax     Decimal(7,2),
    sr_fee                    Decimal(7,2),
    sr_return_ship_cost       Decimal(7,2),
    sr_refunded_cash          Decimal(7,2),
    sr_reversed_charge        Decimal(7,2),
    sr_store_credit           Decimal(7,2),
    sr_net_loss               Decimal(7,2),
    PRIMARY KEY (sr_item_sk, sr_ticket_number)
);

CREATE TABLE store_sales (
    ss_sold_date_sk           UInt32,
    ss_sold_time_sk           UInt32,
    ss_item_sk                Int64 NOT NULL,
    ss_customer_sk            Int64,
    ss_cdemo_sk               Int64,
    ss_hdemo_sk               Int64,
    ss_addr_sk                Int64,
    ss_store_sk               Int64,
    ss_promo_sk               Int64,
    ss_ticket_number          Int64 NOT NULL,
    ss_quantity               Int64,
    ss_wholesale_cost         Decimal(7,2),
    ss_list_price             Decimal(7,2),
    ss_sales_price            Decimal(7,2),
    ss_ext_discount_amt       Decimal(7,2),
    ss_ext_sales_price        Decimal(7,2),
    ss_ext_wholesale_cost     Decimal(7,2),
    ss_ext_list_price         Decimal(7,2),
    ss_ext_tax                Decimal(7,2),
    ss_coupon_amt             Decimal(7,2),
    ss_net_paid               Decimal(7,2),
    ss_net_paid_inc_tax       Decimal(7,2),
    ss_net_profit             Decimal(7,2),
    PRIMARY KEY (ss_item_sk, ss_ticket_number)
);

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
);

CREATE TABLE time_dim (
    t_time_sk                 UInt32 NOT NULL,
    t_time_id                 FixedString(16) NOT NULL,
    t_time                    Int64 NOT NULL,
    t_hour                    Int64,
    t_minute                  Int64,
    t_second                  Int64,
    t_am_pm                   FixedString(2),
    t_shift                   FixedString(20),
    t_sub_shift               FixedString(20),
    t_meal_time               FixedString(20),
    PRIMARY KEY (t_time_sk)
);

CREATE TABLE warehouse(
      w_warehouse_sk            Int64 NOT NULL,
      w_warehouse_id            FixedString(16) NOT NULL,
      w_warehouse_name          String,
      w_warehouse_sq_ft         Int64,
      w_street_number           FixedString(10),
      w_street_name             String,
      w_street_type             FixedString(15),
      w_suite_number            FixedString(10),
      w_city                    String,
      w_county                  String,
      w_state                   FixedString(2),
      w_zip                     FixedString(10),
      w_country                 String,
      w_gmt_offset              Decimal(5,2),
      PRIMARY KEY (w_warehouse_sk)
);

CREATE TABLE web_page(
      wp_web_page_sk            Int64 NOT NULL,
      wp_web_page_id            FixedString(16) NOT NULL,
      wp_rec_start_date         Date,
      wp_rec_end_date           Date,
      wp_creation_date_sk       UInt32,
      wp_access_date_sk         UInt32,
      wp_autogen_flag           FixedString(1),
      wp_customer_sk            Int64,
      wp_url                    String,
      wp_type                   FixedString(50),
      wp_char_count             Int64,
      wp_link_count             Int64,
      wp_image_count            Int64,
      wp_max_ad_count           Int64,
      PRIMARY KEY (wp_web_page_sk)
);

CREATE TABLE web_returns (
    wr_returned_date_sk       UInt32,
    wr_returned_time_sk       UInt32,
    wr_item_sk                Int64 NOT NULL,
    wr_refunded_customer_sk   Int64,
    wr_refunded_cdemo_sk      Int64,
    wr_refunded_hdemo_sk      Int64,
    wr_refunded_addr_sk       Int64,
    wr_returning_customer_sk  Int64,
    wr_returning_cdemo_sk     Int64,
    wr_returning_hdemo_sk     Int64,
    wr_returning_addr_sk      Int64,
    wr_web_page_sk            Int64,
    wr_reason_sk              Int64,
    wr_order_number           Int64 NOT NULL,
    wr_return_quantity        Int64,
    wr_return_amt             Decimal(7,2),
    wr_return_tax             Decimal(7,2),
    wr_return_amt_inc_tax     Decimal(7,2),
    wr_fee                    Decimal(7,2),
    wr_return_ship_cost       Decimal(7,2),
    wr_refunded_cash          Decimal(7,2),
    wr_reversed_charge        Decimal(7,2),
    wr_account_credit         Decimal(7,2),
    wr_net_loss               Decimal(7,2),
    PRIMARY KEY (wr_item_sk, wr_order_number)
);

CREATE TABLE web_sales (
    ws_sold_date_sk           UInt32,
    ws_sold_time_sk           UInt32,
    ws_ship_date_sk           UInt32,
    ws_item_sk                Int64 NOT NULL,
    ws_bill_customer_sk       Int64,
    ws_bill_cdemo_sk          Int64,
    ws_bill_hdemo_sk          Int64,
    ws_bill_addr_sk           Int64,
    ws_ship_customer_sk       Int64,
    ws_ship_cdemo_sk          Int64,
    ws_ship_hdemo_sk          Int64,
    ws_ship_addr_sk           Int64,
    ws_web_page_sk            Int64,
    ws_web_site_sk            Int64,
    ws_ship_mode_sk           Int64,
    ws_warehouse_sk           Int64,
    ws_promo_sk               Int64,
    ws_order_number           Int64 NOT NULL,
    ws_quantity               Int64,
    ws_wholesale_cost         Decimal(7,2),
    ws_list_price             Decimal(7,2),
    ws_sales_price            Decimal(7,2),
    ws_ext_discount_amt       Decimal(7,2),
    ws_ext_sales_price        Decimal(7,2),
    ws_ext_wholesale_cost     Decimal(7,2),
    ws_ext_list_price         Decimal(7,2),
    ws_ext_tax                Decimal(7,2),
    ws_coupon_amt             Decimal(7,2),
    ws_ext_ship_cost          Decimal(7,2),
    ws_net_paid               Decimal(7,2),
    ws_net_paid_inc_tax       Decimal(7,2),
    ws_net_paid_inc_ship      Decimal(7,2),
    ws_net_paid_inc_ship_tax  Decimal(7,2),
    ws_net_profit             Decimal(7,2),
    PRIMARY KEY (ws_item_sk, ws_order_number)
);

CREATE TABLE web_site (
    web_site_sk           Int64 NOT NULL,
    web_site_id           FixedString(16) NOT NULL,
    web_rec_start_date    Date,
    web_rec_end_date      Date,
    web_name              String,
    web_open_date_sk      UInt32,
    web_close_date_sk     UInt32,
    web_class             String,
    web_manager           String,
    web_mkt_id            Int64,
    web_mkt_class         String,
    web_mkt_desc          String,
    web_market_manager    String,
    web_company_id        Int64,
    web_company_name      FixedString(50),
    web_street_number     FixedString(10),
    web_street_name       String,
    web_street_type       FixedString(15),
    web_suite_number      FixedString(10),
    web_city              String,
    web_county            String,
    web_state             FixedString(2),
    web_zip               FixedString(10),
    web_country           String,
    web_gmt_offset        Decimal(5,2),
    web_tax_percentage    Decimal(5,2),
    PRIMARY KEY (web_site_sk)
);