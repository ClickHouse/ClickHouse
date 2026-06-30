DROP TYPE IF EXISTS ProductId;
DROP TYPE IF EXISTS ProductName;
DROP TYPE IF EXISTS ProductList;
DROP TYPE IF EXISTS KeyValue;
DROP TYPE IF EXISTS NestedData;
DROP TABLE IF EXISTS test.products;
DROP TABLE IF EXISTS test.complex_products;

CREATE TYPE ProductId AS UInt64 DEFAULT '0';
CREATE TYPE ProductName AS String DEFAULT 'Unknown Product';

CREATE TYPE ProductList(T) AS Array(T);
CREATE TYPE KeyValue(K, V) AS Tuple(K, V);
CREATE TYPE NestedData(T, U) AS Tuple(Array(T), Map(String, U));

CREATE TABLE test.products (
    id ProductId,
    name ProductName,
    price Float64
) ENGINE = Memory;

INSERT INTO test.products VALUES (1, 'Laptop', 999.99);
INSERT INTO test.products VALUES (2, 'Mouse', 25.50);

CREATE TABLE test.complex_products (
    id ProductId,
    name ProductName,
    tags ProductList(String),
    metadata KeyValue(String, Float64),
    inventory_data NestedData(UInt32, String),
    price Float64
) ENGINE = Memory;

INSERT INTO test.complex_products VALUES 
(1, 'Gaming Laptop', ['electronics', 'gaming', 'computers'], ('weight', 2.5), ([100, 50, 25], {'warehouse_A': 'available', 'warehouse_B': 'limited'}), 1599.99);

INSERT INTO test.complex_products VALUES 
(2, 'Wireless Mouse', ['electronics', 'peripherals'], ('battery_life', 12.0), ([200], {'warehouse_A': 'in_stock'}), 45.99);

INSERT INTO test.complex_products VALUES 
(3, 'Mechanical Keyboard', ['electronics', 'peripherals', 'gaming'], ('switch_type', 1.0), ([75, 30], {'warehouse_A': 'available', 'warehouse_C': 'out_of_stock'}), 129.99);

SELECT * FROM test.products ORDER BY id;
SELECT * FROM test.complex_products ORDER BY id;

SELECT name, type FROM system.columns 
WHERE database = 'test' AND table = 'products' 
ORDER BY name;

SELECT name, type FROM system.columns 
WHERE database = 'test' AND table = 'complex_products' 
ORDER BY name;

SELECT 
    name, 
    tags,
    metadata.1 as metadata_key,
    metadata.2 as metadata_value,
    inventory_data.1 as quantities,
    inventory_data.2 as warehouse_status
FROM test.complex_products 
WHERE has(tags, 'gaming') 
ORDER BY id;

SELECT * FROM system.user_defined_types ORDER BY name;

DROP TABLE test.complex_products;
DROP TABLE test.products;
DROP TYPE NestedData;
DROP TYPE KeyValue;
DROP TYPE ProductList;
DROP TYPE ProductName;
DROP TYPE ProductId;
