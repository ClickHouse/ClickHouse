DROP TABLE IF EXISTS aboba;
CREATE TABLE aboba (
    s String,
    ui UInt8 CODEC(GCD, ZSTD)
)
ENGINE = Memory;
INSERT INTO aboba VALUES 
    ('Hello', 1263),
    ('World', 0),
    ('Goodbye', 293);
SELECT ui FROM aboba;