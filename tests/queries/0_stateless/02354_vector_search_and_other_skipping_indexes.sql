-- Tags: no-fasttest, no-ordinary-database

-- Usage of vector similarity index and further skipping indexes on the same table (issue #71381)

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(
  val String,
  vec Array(Float32),
  INDEX ann_idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 1),
  INDEX set_idx val TYPE set(100)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES ('hello world', [0.0]);

DROP TABLE tab;
