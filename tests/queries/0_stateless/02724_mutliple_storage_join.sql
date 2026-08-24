CREATE TABLE user(id UInt32, name String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO user VALUES (1,'U1')(2,'U2')(3,'U3');

CREATE TABLE product(id UInt32, name String, cate String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO product VALUES (1,'P1','C1')(2,'P2','C1')(3,'P3','C2');

CREATE TABLE order(id UInt32, pId UInt32, uId UInt32) ENGINE = TinyLog;
INSERT INTO order VALUES (1,1,1)(2,1,2)(3,2,3);

SELECT ignore(*) FROM (
    SELECT
        uId,
        user.id as `uuu`
    FROM order
    LEFT ANY JOIN user
    ON uId = `uuu`
);

SELECT ignore(*) FROM order
LEFT ANY JOIN user ON uId = user.id
LEFT ANY JOIN product ON pId = product.id;
