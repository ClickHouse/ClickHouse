DROP TABLE IF EXISTS view_order_attribution;
DROP TABLE IF EXISTS order_attribution;

CREATE TABLE order_attribution
(
    `order_product_event_id` String,
    `order_id` String,
    `brand_id` String,
    `user_id` String,
    `gmv` Float64,
    `gsv` Float64,
    `po_created_at` DateTime64(9, 'UTC'),
    `event_brand_id` Nullable(String),
    `campaign_id` UInt32,
    `event_bid_type` String,
    `event_bid_value` Nullable(Float64),
    `event_inventory_id` UInt32,
    `event_user_id` String,
    `ad_event_id` String,
    `latest_ad_created_at` DateTime64(9, 'UTC'),
    `event_type` String,
    `attribution_window` UInt8,
    `_version` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (brand_id, campaign_id, event_type, latest_ad_created_at, user_id, order_product_event_id, ad_event_id)
SETTINGS index_granularity = 8192;

CREATE VIEW view_order_attribution
(
    `order_product_event_id` String,
    `order_id` String,
    `brand_id` String,
    `user_id` String,
    `sp_id` Nullable(String),
    `gmv` Float64,
    `gsv` Float64,
    `po_created_at` DateTime64(9, 'UTC'),
    `event_brand_id` Nullable(String),
    `campaign_id` UInt32,
    `event_bid_type` String,
    `event_bid_value` Nullable(Float64),
    `event_inventory_id` UInt32,
    `event_user_id` String,
    `ad_event_id` String,
    `event_type` String,
    `latest_ad_created_at` DateTime64(9, 'UTC')
) AS
SELECT
    order_product_event_id,
    order_id,
    brand_id,
    user_id,
    sp_id,
    gmv,
    gsv,
    po_created_at,
    argMax(event_brand_id, event_rank) AS event_brand_id,
    argMax(campaign_id, event_rank) AS campaign_id,
    argMax(event_bid_type, event_rank) AS event_bid_type,
    argMax(event_bid_value, event_rank) AS event_bid_value,
    argMax(event_inventory_id, event_rank) AS event_inventory_id,
    argMax(event_user_id, event_rank) AS event_user_id,
    argMax(ad_event_id, event_rank) AS ad_event_id,
    argMax(event_type, event_rank) AS event_type,
    argMax(latest_ad_created_at, event_rank) AS latest_ad_created_at
FROM
(
    SELECT
        order_product_event_id,
        order_id,
        brand_id,
        user_id,
        sp_id,
        gmv,
        gsv,
        po_created_at,
        event_type,
        if(event_type = 'click', 1, 0) AS event_rank,
        argMax(event_brand_id, _version) AS event_brand_id,
        argMax(campaign_id, _version) AS campaign_id,
        argMax(event_bid_type, _version) AS event_bid_type,
        argMax(event_bid_value, _version) AS event_bid_value,
        argMax(event_inventory_id, _version) AS event_inventory_id,
        argMax(event_user_id, _version) AS event_user_id,
        argMax(ad_event_id, _version) AS ad_event_id,
        argMax(latest_ad_created_at, _version) AS latest_ad_created_at
    FROM analytics.order_attribution
    WHERE attribution_window <= {attr_window:UInt32}
    GROUP BY
        order_product_event_id,
        order_id,
        brand_id,
        user_id,
        sp_id,
        gmv,
        gsv,
        po_created_at,
        event_type
)
GROUP BY
    order_product_event_id,
    order_id,
    brand_id,
    user_id,
    sp_id,
    gmv,
    gsv,
    po_created_at;

DETACH TABLE view_order_attribution;
ATTACH TABLE view_order_attribution;
DROP TABLE view_order_attribution;
DROP TABLE order_attribution;
