-- Schema for the Join Order Benchmark (JOB).
--
-- JOB runs on a snapshot of the IMDB dataset and was introduced in the paper
-- "How Good Are Query Optimizers, Really?" (Leis et al., VLDB 2015).
--
-- Types mapping (from the original PostgreSQL schema):
--   integer NOT NULL          -> Int32
--   integer (nullable)        -> Nullable(Int32)
--   text / character varying  -> String, or Nullable(String) when the column is nullable
--
-- The original schema declares `id integer NOT NULL PRIMARY KEY` for every table; this maps
-- to the sorting key `ORDER BY id` in ClickHouse.

CREATE TABLE aka_name (
    id Int32,
    person_id Int32,
    name String,
    imdb_index Nullable(String),
    name_pcode_cf Nullable(String),
    name_pcode_nf Nullable(String),
    surname_pcode Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE aka_title (
    id Int32,
    movie_id Int32,
    title String,
    imdb_index Nullable(String),
    kind_id Int32,
    production_year Nullable(Int32),
    phonetic_code Nullable(String),
    episode_of_id Nullable(Int32),
    season_nr Nullable(Int32),
    episode_nr Nullable(Int32),
    note Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE cast_info (
    id Int32,
    person_id Int32,
    movie_id Int32,
    person_role_id Nullable(Int32),
    note Nullable(String),
    nr_order Nullable(Int32),
    role_id Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE char_name (
    id Int32,
    name String,
    imdb_index Nullable(String),
    imdb_id Nullable(Int32),
    name_pcode_nf Nullable(String),
    surname_pcode Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE comp_cast_type (
    id Int32,
    kind String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE company_name (
    id Int32,
    name String,
    country_code Nullable(String),
    imdb_id Nullable(Int32),
    name_pcode_nf Nullable(String),
    name_pcode_sf Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE company_type (
    id Int32,
    kind String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE complete_cast (
    id Int32,
    movie_id Nullable(Int32),
    subject_id Int32,
    status_id Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE info_type (
    id Int32,
    info String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE keyword (
    id Int32,
    keyword String,
    phonetic_code Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE kind_type (
    id Int32,
    kind String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE link_type (
    id Int32,
    link String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE movie_companies (
    id Int32,
    movie_id Int32,
    company_id Int32,
    company_type_id Int32,
    note Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE movie_info (
    id Int32,
    movie_id Int32,
    info_type_id Int32,
    info String,
    note Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE movie_info_idx (
    id Int32,
    movie_id Int32,
    info_type_id Int32,
    info String,
    note Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE movie_keyword (
    id Int32,
    movie_id Int32,
    keyword_id Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE movie_link (
    id Int32,
    movie_id Int32,
    linked_movie_id Int32,
    link_type_id Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE name (
    id Int32,
    name String,
    imdb_index Nullable(String),
    imdb_id Nullable(Int32),
    gender Nullable(String),
    name_pcode_cf Nullable(String),
    name_pcode_nf Nullable(String),
    surname_pcode Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE person_info (
    id Int32,
    person_id Int32,
    info_type_id Int32,
    info String,
    note Nullable(String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE role_type (
    id Int32,
    role String)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE title (
    id Int32,
    title String,
    imdb_index Nullable(String),
    kind_id Int32,
    production_year Nullable(Int32),
    imdb_id Nullable(Int32),
    phonetic_code Nullable(String),
    episode_of_id Nullable(Int32),
    season_nr Nullable(Int32),
    episode_nr Nullable(Int32),
    series_years Nullable(String),
    md5sum Nullable(String))
ENGINE = MergeTree ORDER BY id;
