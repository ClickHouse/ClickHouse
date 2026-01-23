SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS AddedToCart;
DROP TABLE IF EXISTS Session;

CREATE TABLE Session
(
    id String,
    site Enum8('STORE_A' = 1, 'STORE_B' = 2),
    device Enum8('DESKTOP' = 1, 'MOBILE' = 2)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE AddedToCart
(
    sessionId String,
    order Int32,
    top Nullable(Int32),
    screenHeight Nullable(Int32),
    screenWidth Nullable(Int32),
    isPromotion UInt8,
    date DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (sessionId, date);

INSERT INTO Session (id, site, device) VALUES
    ('s1', 'STORE_A', 'DESKTOP'),
    ('s2', 'STORE_B', 'MOBILE');

INSERT INTO AddedToCart (sessionId, order, top, screenHeight, screenWidth, isPromotion, date) VALUES
    ('s1', 1, 100, 400, 1024, 1, parseDateTime64BestEffort('2026-01-19T12:00:00.000Z', 3)),
    ('s2', 2, 100, 400, 1024, 1, parseDateTime64BestEffort('2026-01-19T12:00:01.000Z', 3));

SELECT
    s.site AS site,
    if((a.order IS NULL) OR (a.order <= 0) OR (a.order > 30), NULL, accurateCastOrNull(a.order, 'Int32')) AS page_level,
    count() AS count
FROM AddedToCart AS a
ANY LEFT JOIN Session AS s ON a.sessionId = s.id
WHERE (a.top IS NOT NULL)
  AND (a.screenHeight IS NOT NULL)
  AND (a.screenHeight > 0)
  AND (a.isPromotion = _CAST(1, 'UInt8'))
  AND (s.device = 'DESKTOP')
  AND isNotNull(s.site)
GROUP BY site, page_level
ORDER BY site ASC, page_level ASC
FORMAT JSONEachRow;

DROP TABLE AddedToCart;
DROP TABLE Session;
