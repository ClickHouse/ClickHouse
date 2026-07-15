DROP TABLE IF EXISTS lance_local_readonly_mvp;

CREATE TABLE lance_local_readonly_mvp
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance');

SELECT count() FROM lance_local_readonly_mvp;
SELECT id, name FROM lance_local_readonly_mvp ORDER BY id;
SELECT id FROM lance_local_readonly_mvp WHERE id = 2;
SELECT score FROM lance_local_readonly_mvp WHERE id = 2;

DROP TABLE lance_local_readonly_mvp;
