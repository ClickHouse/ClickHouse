--
-- PostgreSQL database dump
--

\restrict 0MgBS2FGsf7MPclRqhdJyRAFEJaEFd5d1lwFwwaADzKla8dRSjKffLJWzxQMdyf

-- Dumped from database version 16.14 (Debian 16.14-1.pgdg13+1)
-- Dumped by pg_dump version 16.14 (Debian 16.14-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ducklake_column; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column (
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    table_id bigint,
    column_order bigint,
    column_name character varying,
    column_type character varying,
    initial_default character varying,
    default_value character varying,
    nulls_allowed boolean,
    parent_column bigint,
    default_value_type character varying,
    default_value_dialect character varying
);


--
-- Name: ducklake_column_mapping; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column_mapping (
    mapping_id bigint,
    table_id bigint,
    type character varying
);


--
-- Name: ducklake_column_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_column_tag (
    table_id bigint,
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


--
-- Name: ducklake_data_file; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_data_file (
    data_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    file_order bigint,
    path character varying,
    path_is_relative boolean,
    file_format character varying,
    record_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    row_id_start bigint,
    partition_id bigint,
    encryption_key character varying,
    mapping_id bigint,
    partial_max bigint
);


--
-- Name: ducklake_delete_file; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_delete_file (
    delete_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    format character varying,
    delete_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    encryption_key character varying,
    partial_max bigint
);


--
-- Name: ducklake_file_column_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_column_stats (
    data_file_id bigint,
    table_id bigint,
    column_id bigint,
    column_size_bytes bigint,
    value_count bigint,
    null_count bigint,
    min_value character varying,
    max_value character varying,
    contains_nan boolean,
    extra_stats character varying
);


--
-- Name: ducklake_file_partition_value; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_partition_value (
    data_file_id bigint,
    table_id bigint,
    partition_key_index bigint,
    partition_value character varying
);


--
-- Name: ducklake_file_variant_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_file_variant_stats (
    data_file_id bigint,
    table_id bigint,
    column_id bigint,
    variant_path character varying,
    shredded_type character varying,
    column_size_bytes bigint,
    value_count bigint,
    null_count bigint,
    min_value character varying,
    max_value character varying,
    contains_nan boolean,
    extra_stats character varying
);


--
-- Name: ducklake_files_scheduled_for_deletion; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_files_scheduled_for_deletion (
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    schedule_start timestamp with time zone
);


--
-- Name: ducklake_inlined_data_3_6; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_3_6 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    v bytea
);


--
-- Name: ducklake_inlined_data_6_7; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_6_7 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    b boolean,
    i8 smallint,
    i16 smallint,
    i32 integer,
    i64 bigint,
    h character varying,
    u8 integer,
    u16 integer,
    u32 bigint,
    u64 character varying,
    f32 real,
    f64 double precision,
    d numeric(10,2),
    vc bytea,
    bl bytea,
    dt character varying,
    tm time without time zone,
    ts character varying,
    tstz character varying,
    u uuid
);


--
-- Name: ducklake_inlined_data_7_8; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_7_8 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    s character varying,
    l character varying,
    m character varying
);


--
-- Name: ducklake_inlined_data_8_10; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_8_10 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a bytea,
    b double precision
);


--
-- Name: ducklake_inlined_data_8_11; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_8_11 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a2 bytea,
    b double precision
);


--
-- Name: ducklake_inlined_data_8_9; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_8_9 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a bytea
);


--
-- Name: ducklake_inlined_data_tables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_tables (
    table_id bigint,
    table_name character varying,
    schema_version bigint
);


--
-- Name: ducklake_inlined_delete_3; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_delete_3 (
    file_id bigint,
    row_id bigint,
    begin_snapshot bigint
);


--
-- Name: ducklake_macro; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro (
    schema_id bigint,
    macro_id bigint,
    macro_name character varying,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_macro_impl; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro_impl (
    macro_id bigint,
    impl_id bigint,
    dialect character varying,
    sql character varying,
    type character varying
);


--
-- Name: ducklake_macro_parameters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_macro_parameters (
    macro_id bigint,
    impl_id bigint,
    column_id bigint,
    parameter_name character varying,
    parameter_type character varying,
    default_value character varying,
    default_value_type character varying
);


--
-- Name: ducklake_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_metadata (
    key character varying NOT NULL,
    value character varying NOT NULL,
    scope character varying,
    scope_id bigint
);


--
-- Name: ducklake_name_mapping; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_name_mapping (
    mapping_id bigint,
    column_id bigint,
    source_name character varying,
    target_field_id bigint,
    parent_column bigint,
    is_partition boolean
);


--
-- Name: ducklake_partition_column; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_partition_column (
    partition_id bigint,
    table_id bigint,
    partition_key_index bigint,
    column_id bigint,
    transform character varying
);


--
-- Name: ducklake_partition_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_partition_info (
    partition_id bigint,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_schema; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_schema (
    schema_id bigint NOT NULL,
    schema_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_name character varying,
    path character varying,
    path_is_relative boolean
);


--
-- Name: ducklake_schema_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_schema_versions (
    begin_snapshot bigint,
    schema_version bigint,
    table_id bigint
);


--
-- Name: ducklake_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_snapshot (
    snapshot_id bigint NOT NULL,
    snapshot_time timestamp with time zone,
    schema_version bigint,
    next_catalog_id bigint,
    next_file_id bigint
);


--
-- Name: ducklake_snapshot_changes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_snapshot_changes (
    snapshot_id bigint NOT NULL,
    changes_made character varying,
    author character varying,
    commit_message character varying,
    commit_extra_info character varying
);


--
-- Name: ducklake_sort_expression; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_sort_expression (
    sort_id bigint,
    table_id bigint,
    sort_key_index bigint,
    expression character varying,
    dialect character varying,
    sort_direction character varying,
    null_order character varying
);


--
-- Name: ducklake_sort_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_sort_info (
    sort_id bigint,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint
);


--
-- Name: ducklake_table; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table (
    table_id bigint,
    table_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    table_name character varying,
    path character varying,
    path_is_relative boolean
);


--
-- Name: ducklake_table_column_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table_column_stats (
    table_id bigint,
    column_id bigint,
    contains_null boolean,
    contains_nan boolean,
    min_value character varying,
    max_value character varying,
    extra_stats character varying
);


--
-- Name: ducklake_table_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_table_stats (
    table_id bigint,
    record_count bigint,
    next_row_id bigint,
    file_size_bytes bigint
);


--
-- Name: ducklake_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_tag (
    object_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


--
-- Name: ducklake_view; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_view (
    view_id bigint,
    view_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    view_name character varying,
    dialect character varying,
    sql character varying,
    column_aliases character varying
);


--
-- Data for Name: ducklake_column; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type, default_value_dialect) FROM stdin;
1	1	\N	1	1	region	varchar	\N	NULL	t	\N	literal	duckdb
2	1	\N	1	2	dt	date	\N	NULL	t	\N	literal	duckdb
3	1	\N	1	3	id	int32	\N	NULL	t	\N	literal	duckdb
4	1	\N	1	4	val	varchar	\N	NULL	t	\N	literal	duckdb
1	9	\N	3	1	id	int32	\N	NULL	t	\N	literal	duckdb
2	9	\N	3	2	v	varchar	\N	NULL	t	\N	literal	duckdb
1	11	\N	4	1	ts	timestamptz	\N	NULL	t	\N	literal	duckdb
2	11	\N	4	2	id	int32	\N	NULL	t	\N	literal	duckdb
3	11	\N	4	3	val	varchar	\N	NULL	t	\N	literal	duckdb
1	35	\N	6	1	b	boolean	\N	NULL	t	\N	literal	duckdb
2	35	\N	6	2	i8	int8	\N	NULL	t	\N	literal	duckdb
3	35	\N	6	3	i16	int16	\N	NULL	t	\N	literal	duckdb
4	35	\N	6	4	i32	int32	\N	NULL	t	\N	literal	duckdb
5	35	\N	6	5	i64	int64	\N	NULL	t	\N	literal	duckdb
6	35	\N	6	6	h	int128	\N	NULL	t	\N	literal	duckdb
7	35	\N	6	7	u8	uint8	\N	NULL	t	\N	literal	duckdb
8	35	\N	6	8	u16	uint16	\N	NULL	t	\N	literal	duckdb
9	35	\N	6	9	u32	uint32	\N	NULL	t	\N	literal	duckdb
10	35	\N	6	10	u64	uint64	\N	NULL	t	\N	literal	duckdb
11	35	\N	6	11	f32	float32	\N	NULL	t	\N	literal	duckdb
12	35	\N	6	12	f64	float64	\N	NULL	t	\N	literal	duckdb
13	35	\N	6	13	d	decimal(10,2)	\N	NULL	t	\N	literal	duckdb
14	35	\N	6	14	vc	varchar	\N	NULL	t	\N	literal	duckdb
15	35	\N	6	15	bl	blob	\N	NULL	t	\N	literal	duckdb
16	35	\N	6	16	dt	date	\N	NULL	t	\N	literal	duckdb
17	35	\N	6	17	tm	time	\N	NULL	t	\N	literal	duckdb
18	35	\N	6	18	ts	timestamp	\N	NULL	t	\N	literal	duckdb
19	35	\N	6	19	tstz	timestamptz	\N	NULL	t	\N	literal	duckdb
20	35	\N	6	20	u	uuid	\N	NULL	t	\N	literal	duckdb
1	37	\N	7	1	id	int32	\N	NULL	t	\N	literal	duckdb
2	37	\N	7	2	s	struct	\N	NULL	t	\N		duckdb
3	37	\N	7	3	x	int32	\N	NULL	t	2	literal	duckdb
4	37	\N	7	4	y	varchar	\N	NULL	t	2	literal	duckdb
5	37	\N	7	5	l	list	\N	NULL	t	\N		duckdb
6	37	\N	7	6	element	int32	\N	NULL	t	5	literal	duckdb
7	37	\N	7	7	m	map	\N	NULL	t	\N		duckdb
8	37	\N	7	8	key	varchar	\N	NULL	t	7	literal	duckdb
9	37	\N	7	9	value	int32	\N	NULL	t	7	literal	duckdb
1	39	\N	8	1	id	int32	\N	NULL	t	\N	literal	duckdb
3	41	\N	8	3	b	float64	\N	NULL	t	\N	literal	duckdb
2	39	43	8	2	a	varchar	\N	NULL	t	\N	literal	duckdb
2	43	\N	8	2	a2	varchar	\N	NULL	t	\N	literal	duckdb
\.


--
-- Data for Name: ducklake_column_mapping; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_column_mapping (mapping_id, table_id, type) FROM stdin;
\.


--
-- Data for Name: ducklake_column_tag; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_column_tag (table_id, column_id, begin_snapshot, end_snapshot, key, value) FROM stdin;
\.


--
-- Data for Name: ducklake_data_file; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) FROM stdin;
0	1	3	\N	\N	region=a/year=2023/ducklake-019f75dd-c82f-71b7-ab5c-af82d82304a4.parquet	t	parquet	20	815	377	0	2	\N	\N	\N
1	1	4	\N	\N	region=a/year=2024/ducklake-019f75dd-c83e-7cb3-aa96-4819526ddfde.parquet	t	parquet	20	824	381	20	2	\N	\N	\N
2	1	5	\N	\N	region=b/year=2023/ducklake-019f75dd-c84c-7788-8201-8ee6ccd910cf.parquet	t	parquet	20	824	381	40	2	\N	\N	\N
3	1	6	\N	\N	region=b/year=2024/ducklake-019f75dd-c857-729a-aab6-7d492c6d4869.parquet	t	parquet	20	824	381	60	2	\N	\N	\N
4	1	7	\N	\N	region=c/year=2023/ducklake-019f75dd-c862-7b0d-bacb-9feb005c1912.parquet	t	parquet	20	824	381	80	2	\N	\N	\N
5	1	8	\N	\N	region=c/year=2024/ducklake-019f75dd-c86f-7151-bf0a-7d4b6571445a.parquet	t	parquet	20	824	385	100	2	\N	\N	\N
6	3	10	\N	\N	ducklake-019f75dd-c8a4-7f4a-8de5-e6e55cb5c52c.parquet	t	parquet	100	1128	241	0	\N	\N	\N	\N
7	4	13	\N	\N	year=2023/month=1/day=1/ducklake-019f75dd-c90f-78e7-a0b7-e0e6820023d7.parquet	t	parquet	2	472	339	0	5	\N	\N	\N
8	4	14	\N	\N	year=2023/month=1/day=15/ducklake-019f75dd-c917-7333-9d6b-bd0ea8405cdf.parquet	t	parquet	2	472	339	2	5	\N	\N	\N
9	4	15	\N	\N	year=2023/month=6/day=1/ducklake-019f75dd-c921-732c-a16e-10bd79fd46eb.parquet	t	parquet	2	472	339	4	5	\N	\N	\N
10	4	16	\N	\N	year=2023/month=6/day=15/ducklake-019f75dd-c932-77c7-aa3d-f91e12f3df84.parquet	t	parquet	2	472	339	6	5	\N	\N	\N
11	4	17	\N	\N	year=2024/month=1/day=1/ducklake-019f75dd-c940-7c7b-a6c6-526b7ae82411.parquet	t	parquet	2	472	339	8	5	\N	\N	\N
12	4	18	\N	\N	year=2024/month=1/day=15/ducklake-019f75dd-c949-71b6-95ab-e80918ba3b5a.parquet	t	parquet	2	472	339	10	5	\N	\N	\N
13	4	19	\N	\N	year=2024/month=2/day=1/ducklake-019f75dd-c955-7a43-862c-cfe5cc1552de.parquet	t	parquet	2	472	339	12	5	\N	\N	\N
14	4	20	\N	\N	year=2024/month=2/day=15/ducklake-019f75dd-c961-7f3a-8a9c-c0893f934cdb.parquet	t	parquet	2	472	339	14	5	\N	\N	\N
15	4	21	\N	\N	year=2024/month=3/day=1/ducklake-019f75dd-c96d-752d-be3e-261bc18314ff.parquet	t	parquet	2	472	339	16	5	\N	\N	\N
16	4	22	\N	\N	year=2024/month=3/day=15/ducklake-019f75dd-c97a-79d0-a00a-9c843e050ab2.parquet	t	parquet	2	472	339	18	5	\N	\N	\N
17	4	23	\N	\N	year=2024/month=4/day=1/ducklake-019f75dd-c988-7df0-ac47-aa8b1b00b7bb.parquet	t	parquet	2	472	339	20	5	\N	\N	\N
18	4	24	\N	\N	year=2024/month=4/day=15/ducklake-019f75dd-c998-7916-bec8-a3732d09c1e9.parquet	t	parquet	2	472	339	22	5	\N	\N	\N
19	4	25	\N	\N	year=2024/month=5/day=1/ducklake-019f75dd-c9a3-7d8a-b63d-3fb444d36941.parquet	t	parquet	2	472	339	24	5	\N	\N	\N
20	4	26	\N	\N	year=2024/month=5/day=15/ducklake-019f75dd-c9b3-7608-8b30-8cc96dac95b3.parquet	t	parquet	2	472	339	26	5	\N	\N	\N
21	4	27	\N	\N	year=2024/month=6/day=1/ducklake-019f75dd-c9be-7f28-93d3-34085f72ef45.parquet	t	parquet	2	472	339	28	5	\N	\N	\N
22	4	28	\N	\N	year=2024/month=6/day=15/ducklake-019f75dd-c9cb-7aae-9b28-28195ac864f8.parquet	t	parquet	2	472	339	30	5	\N	\N	\N
23	4	29	\N	\N	year=2024/month=7/day=1/ducklake-019f75dd-c9d7-72a6-8de8-ebeb9c584fb3.parquet	t	parquet	2	472	339	32	5	\N	\N	\N
24	4	30	\N	\N	year=2024/month=7/day=15/ducklake-019f75dd-c9e4-74b4-831e-53d46d24ef51.parquet	t	parquet	2	472	339	34	5	\N	\N	\N
\.


--
-- Data for Name: ducklake_delete_file; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key, partial_max) FROM stdin;
\.


--
-- Data for Name: ducklake_file_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) FROM stdin;
0	1	1	48	20	0	a	a	\N	\N
0	1	2	108	20	0	2023-01-01	2023-01-20	\N	\N
0	1	3	105	20	0	0	19	\N	\N
0	1	4	118	20	0	v0	v9	\N	\N
1	1	1	48	20	0	a	a	\N	\N
1	1	2	108	20	0	2024-01-01	2024-01-20	\N	\N
1	1	3	108	20	0	20	39	\N	\N
1	1	4	120	20	0	v20	v39	\N	\N
2	1	1	48	20	0	b	b	\N	\N
2	1	2	108	20	0	2023-01-01	2023-01-20	\N	\N
2	1	3	108	20	0	40	59	\N	\N
2	1	4	120	20	0	v40	v59	\N	\N
3	1	1	48	20	0	b	b	\N	\N
3	1	2	108	20	0	2024-01-01	2024-01-20	\N	\N
3	1	3	108	20	0	60	79	\N	\N
3	1	4	120	20	0	v60	v79	\N	\N
4	1	1	48	20	0	c	c	\N	\N
4	1	2	108	20	0	2023-01-01	2023-01-20	\N	\N
4	1	3	108	20	0	80	99	\N	\N
4	1	4	120	20	0	v80	v99	\N	\N
5	1	1	48	20	0	c	c	\N	\N
5	1	2	108	20	0	2024-01-01	2024-01-20	\N	\N
5	1	3	108	20	0	100	119	\N	\N
5	1	4	116	20	0	v100	v119	\N	\N
6	3	1	430	100	0	0	99	\N	\N
6	3	2	445	100	0	file0	file99	\N	\N
7	4	1	41	2	0	2023-01-01 12:00:00+00	2023-01-01 12:00:00+00	\N	\N
7	4	2	33	2	0	20230101	20230101	\N	\N
7	4	3	47	2	0	v230101	v230101	\N	\N
8	4	1	41	2	0	2023-01-15 12:00:00+00	2023-01-15 12:00:00+00	\N	\N
8	4	2	33	2	0	20230115	20230115	\N	\N
8	4	3	47	2	0	v230115	v230115	\N	\N
9	4	1	41	2	0	2023-06-01 12:00:00+00	2023-06-01 12:00:00+00	\N	\N
9	4	2	33	2	0	20230601	20230601	\N	\N
9	4	3	47	2	0	v230601	v230601	\N	\N
10	4	1	41	2	0	2023-06-15 12:00:00+00	2023-06-15 12:00:00+00	\N	\N
10	4	2	33	2	0	20230615	20230615	\N	\N
10	4	3	47	2	0	v230615	v230615	\N	\N
11	4	1	41	2	0	2024-01-01 12:00:00+00	2024-01-01 12:00:00+00	\N	\N
11	4	2	33	2	0	20240101	20240101	\N	\N
11	4	3	47	2	0	v240101	v240101	\N	\N
12	4	1	41	2	0	2024-01-15 12:00:00+00	2024-01-15 12:00:00+00	\N	\N
12	4	2	33	2	0	20240115	20240115	\N	\N
12	4	3	47	2	0	v240115	v240115	\N	\N
13	4	1	41	2	0	2024-02-01 12:00:00+00	2024-02-01 12:00:00+00	\N	\N
13	4	2	33	2	0	20240201	20240201	\N	\N
13	4	3	47	2	0	v240201	v240201	\N	\N
14	4	1	41	2	0	2024-02-15 12:00:00+00	2024-02-15 12:00:00+00	\N	\N
14	4	2	33	2	0	20240215	20240215	\N	\N
14	4	3	47	2	0	v240215	v240215	\N	\N
15	4	1	41	2	0	2024-03-01 12:00:00+00	2024-03-01 12:00:00+00	\N	\N
15	4	2	33	2	0	20240301	20240301	\N	\N
15	4	3	47	2	0	v240301	v240301	\N	\N
16	4	1	41	2	0	2024-03-15 12:00:00+00	2024-03-15 12:00:00+00	\N	\N
16	4	2	33	2	0	20240315	20240315	\N	\N
16	4	3	47	2	0	v240315	v240315	\N	\N
17	4	1	41	2	0	2024-04-01 12:00:00+00	2024-04-01 12:00:00+00	\N	\N
17	4	2	33	2	0	20240401	20240401	\N	\N
17	4	3	47	2	0	v240401	v240401	\N	\N
18	4	1	41	2	0	2024-04-15 12:00:00+00	2024-04-15 12:00:00+00	\N	\N
18	4	2	33	2	0	20240415	20240415	\N	\N
18	4	3	47	2	0	v240415	v240415	\N	\N
19	4	1	41	2	0	2024-05-01 12:00:00+00	2024-05-01 12:00:00+00	\N	\N
19	4	2	33	2	0	20240501	20240501	\N	\N
19	4	3	47	2	0	v240501	v240501	\N	\N
20	4	1	41	2	0	2024-05-15 12:00:00+00	2024-05-15 12:00:00+00	\N	\N
20	4	2	33	2	0	20240515	20240515	\N	\N
20	4	3	47	2	0	v240515	v240515	\N	\N
21	4	1	41	2	0	2024-06-01 12:00:00+00	2024-06-01 12:00:00+00	\N	\N
21	4	2	33	2	0	20240601	20240601	\N	\N
21	4	3	47	2	0	v240601	v240601	\N	\N
22	4	1	41	2	0	2024-06-15 12:00:00+00	2024-06-15 12:00:00+00	\N	\N
22	4	2	33	2	0	20240615	20240615	\N	\N
22	4	3	47	2	0	v240615	v240615	\N	\N
23	4	1	41	2	0	2024-07-01 12:00:00+00	2024-07-01 12:00:00+00	\N	\N
23	4	2	33	2	0	20240701	20240701	\N	\N
23	4	3	47	2	0	v240701	v240701	\N	\N
24	4	1	41	2	0	2024-07-15 12:00:00+00	2024-07-15 12:00:00+00	\N	\N
24	4	2	33	2	0	20240715	20240715	\N	\N
24	4	3	47	2	0	v240715	v240715	\N	\N
\.


--
-- Data for Name: ducklake_file_partition_value; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value) FROM stdin;
0	1	0	a
0	1	1	2023
1	1	0	a
1	1	1	2024
2	1	0	b
2	1	1	2023
3	1	0	b
3	1	1	2024
4	1	0	c
4	1	1	2023
5	1	0	c
5	1	1	2024
7	4	0	2023
7	4	1	1
7	4	2	1
8	4	0	2023
8	4	1	1
8	4	2	15
9	4	0	2023
9	4	1	6
9	4	2	1
10	4	0	2023
10	4	1	6
10	4	2	15
11	4	0	2024
11	4	1	1
11	4	2	1
12	4	0	2024
12	4	1	1
12	4	2	15
13	4	0	2024
13	4	1	2
13	4	2	1
14	4	0	2024
14	4	1	2
14	4	2	15
15	4	0	2024
15	4	1	3
15	4	2	1
16	4	0	2024
16	4	1	3
16	4	2	15
17	4	0	2024
17	4	1	4
17	4	2	1
18	4	0	2024
18	4	1	4
18	4	2	15
19	4	0	2024
19	4	1	5
19	4	2	1
20	4	0	2024
20	4	1	5
20	4	2	15
21	4	0	2024
21	4	1	6
21	4	2	1
22	4	0	2024
22	4	1	6
22	4	2	15
23	4	0	2024
23	4	1	7
23	4	2	1
24	4	0	2024
24	4	1	7
24	4	2	15
\.


--
-- Data for Name: ducklake_file_variant_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_file_variant_stats (data_file_id, table_id, column_id, variant_path, shredded_type, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) FROM stdin;
\.


--
-- Data for Name: ducklake_files_scheduled_for_deletion; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, schedule_start) FROM stdin;
\.


--
-- Data for Name: ducklake_inlined_data_3_6; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_3_6 (row_id, begin_snapshot, end_snapshot, id, v) FROM stdin;
100	32	\N	1000	\\x696e6c30
104	32	\N	1004	\\x696e6c34
101	33	\N	1001	\\x75706461746564
103	33	\N	1003	\\x75706461746564
101	32	33	1001	\\x696e6c31
103	32	33	1003	\\x696e6c33
102	32	34	1002	\\x696e6c32
\.


--
-- Data for Name: ducklake_inlined_data_6_7; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_6_7 (row_id, begin_snapshot, end_snapshot, b, i8, i16, i32, i64, h, u8, u16, u32, u64, f32, f64, d, vc, bl, dt, tm, ts, tstz, u) FROM stdin;
0	36	\N	t	-1	-2	-3	-4	12345	1	2	3	4	1.5	2.5	12.34	\\x737472	\\x626c6f6264617461	2024-01-15	10:30:00	2024-01-15 10:30:00	2024-01-15 10:30:00+00	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
1	36	\N	f	1	2	3	4	-12345	5	6	7	8	-1.5	-2.5	-12.34	\\x7765697264202771756f7465	\\x006666	2025-02-16	11:31:01	2025-02-16 11:31:01.123456	2025-02-16 11:31:01.123456+00	b1ffbc99-9c0b-4ef8-bb6d-6bb9bd380a22
\.


--
-- Data for Name: ducklake_inlined_data_7_8; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_7_8 (row_id, begin_snapshot, end_snapshot, id, s, l, m) FROM stdin;
0	38	\N	1	{'x': 1, 'y': 'u'}	[1, 2]	{a=1}
1	38	\N	2	{'x': 2, 'y': 'v w'}	[3]	{b=2, c=3}
2	38	\N	3	\N	\N	\N
\.


--
-- Data for Name: ducklake_inlined_data_8_10; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_8_10 (row_id, begin_snapshot, end_snapshot, id, a, b) FROM stdin;
2	42	\N	3	\\x7468726565	3.5
3	42	\N	4	\\x666f7572	4.5
\.


--
-- Data for Name: ducklake_inlined_data_8_11; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_8_11 (row_id, begin_snapshot, end_snapshot, id, a2, b) FROM stdin;
4	44	\N	5	\\x66697665	5.5
\.


--
-- Data for Name: ducklake_inlined_data_8_9; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_8_9 (row_id, begin_snapshot, end_snapshot, id, a) FROM stdin;
0	40	\N	1	\\x6f6e65
1	40	\N	2	\\x74776f
\.


--
-- Data for Name: ducklake_inlined_data_tables; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_tables (table_id, table_name, schema_version) FROM stdin;
3	ducklake_inlined_data_3_6	6
6	ducklake_inlined_data_6_7	7
7	ducklake_inlined_data_7_8	8
8	ducklake_inlined_data_8_9	9
8	ducklake_inlined_data_8_10	10
8	ducklake_inlined_data_8_11	11
\.


--
-- Data for Name: ducklake_inlined_delete_3; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_delete_3 (file_id, row_id, begin_snapshot) FROM stdin;
6	3	31
6	17	31
6	42	31
\.


--
-- Data for Name: ducklake_macro; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_macro (schema_id, macro_id, macro_name, begin_snapshot, end_snapshot) FROM stdin;
\.


--
-- Data for Name: ducklake_macro_impl; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_macro_impl (macro_id, impl_id, dialect, sql, type) FROM stdin;
\.


--
-- Data for Name: ducklake_macro_parameters; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_macro_parameters (macro_id, impl_id, column_id, parameter_name, parameter_type, default_value, default_value_type) FROM stdin;
\.


--
-- Data for Name: ducklake_metadata; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_metadata (key, value, scope, scope_id) FROM stdin;
version	1.0	\N	\N
created_by	DuckDB 08e34c447b	\N	\N
encrypted	false	\N	\N
data_path	/var/lib/clickhouse/user_files/ducklake_data2/	\N	\N
\.


--
-- Data for Name: ducklake_name_mapping; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_name_mapping (mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) FROM stdin;
\.


--
-- Data for Name: ducklake_partition_column; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) FROM stdin;
2	1	0	1	identity
2	1	1	2	year
5	4	0	1	year
5	4	1	1	month
5	4	2	1	day
\.


--
-- Data for Name: ducklake_partition_info; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot) FROM stdin;
2	1	2	\N
5	4	12	\N
\.


--
-- Data for Name: ducklake_schema; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) FROM stdin;
0	98cb6221-d370-493d-85ce-3b19679d5dcd	0	\N	main	main/	t
\.


--
-- Data for Name: ducklake_schema_versions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_schema_versions (begin_snapshot, schema_version, table_id) FROM stdin;
1	1	1
2	2	1
9	3	3
11	4	4
12	5	4
35	7	6
37	8	7
39	9	8
41	10	8
43	11	8
\.


--
-- Data for Name: ducklake_snapshot; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) FROM stdin;
0	2026-07-18 15:34:56.466958+00	0	1	0
1	2026-07-18 15:34:56.708022+00	1	2	0
2	2026-07-18 15:34:56.755614+00	2	3	0
3	2026-07-18 15:34:56.79237+00	2	3	1
4	2026-07-18 15:34:56.826372+00	2	3	2
5	2026-07-18 15:34:56.842175+00	2	3	3
6	2026-07-18 15:34:56.853059+00	2	3	4
7	2026-07-18 15:34:56.86471+00	2	3	5
8	2026-07-18 15:34:56.877058+00	2	3	6
9	2026-07-18 15:34:56.887609+00	3	4	6
10	2026-07-18 15:34:56.916668+00	3	4	7
11	2026-07-18 15:34:56.94369+00	4	5	7
12	2026-07-18 15:34:56.975416+00	5	6	7
13	2026-07-18 15:34:57.021169+00	5	6	8
14	2026-07-18 15:34:57.046353+00	5	6	9
15	2026-07-18 15:34:57.056048+00	5	6	10
16	2026-07-18 15:34:57.070833+00	5	6	11
17	2026-07-18 15:34:57.08598+00	5	6	12
18	2026-07-18 15:34:57.096302+00	5	6	13
19	2026-07-18 15:34:57.104307+00	5	6	14
20	2026-07-18 15:34:57.119348+00	5	6	15
21	2026-07-18 15:34:57.130899+00	5	6	16
22	2026-07-18 15:34:57.14466+00	5	6	17
23	2026-07-18 15:34:57.158248+00	5	6	18
24	2026-07-18 15:34:57.172764+00	5	6	19
25	2026-07-18 15:34:57.18534+00	5	6	20
26	2026-07-18 15:34:57.199937+00	5	6	21
27	2026-07-18 15:34:57.213399+00	5	6	22
28	2026-07-18 15:34:57.223185+00	5	6	23
29	2026-07-18 15:34:57.237372+00	5	6	24
30	2026-07-18 15:34:57.248954+00	5	6	25
31	2026-07-18 15:34:57.286791+00	5	6	25
32	2026-07-18 15:34:57.331054+00	6	6	26
33	2026-07-18 15:34:57.346353+00	6	6	27
34	2026-07-18 15:34:57.404462+00	6	6	27
35	2026-07-18 15:34:57.424883+00	7	7	27
36	2026-07-18 15:34:57.462594+00	7	7	28
37	2026-07-18 15:34:57.498617+00	8	8	28
38	2026-07-18 15:34:57.536817+00	8	8	29
39	2026-07-18 15:34:57.57446+00	9	9	29
40	2026-07-18 15:34:57.607515+00	9	9	30
41	2026-07-18 15:34:57.640695+00	10	9	30
42	2026-07-18 15:34:57.678816+00	10	9	31
43	2026-07-18 15:34:57.71076+00	11	9	31
44	2026-07-18 15:34:57.745406+00	11	9	32
\.


--
-- Data for Name: ducklake_snapshot_changes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message, commit_extra_info) FROM stdin;
0	created_schema:"main"	\N	\N	\N
1	created_table:"main"."partitioned"	\N	\N	\N
2	altered_table:1	\N	\N	\N
3	inserted_into_table:1	\N	\N	\N
4	inserted_into_table:1	\N	\N	\N
5	inserted_into_table:1	\N	\N	\N
6	inserted_into_table:1	\N	\N	\N
7	inserted_into_table:1	\N	\N	\N
8	inserted_into_table:1	\N	\N	\N
9	created_table:"main"."inlined_mixed"	\N	\N	\N
10	inserted_into_table:3	\N	\N	\N
11	created_table:"main"."partitioned_cal"	\N	\N	\N
12	altered_table:4	\N	\N	\N
13	inserted_into_table:4	\N	\N	\N
14	inserted_into_table:4	\N	\N	\N
15	inserted_into_table:4	\N	\N	\N
16	inserted_into_table:4	\N	\N	\N
17	inserted_into_table:4	\N	\N	\N
18	inserted_into_table:4	\N	\N	\N
19	inserted_into_table:4	\N	\N	\N
20	inserted_into_table:4	\N	\N	\N
21	inserted_into_table:4	\N	\N	\N
22	inserted_into_table:4	\N	\N	\N
23	inserted_into_table:4	\N	\N	\N
24	inserted_into_table:4	\N	\N	\N
25	inserted_into_table:4	\N	\N	\N
26	inserted_into_table:4	\N	\N	\N
27	inserted_into_table:4	\N	\N	\N
28	inserted_into_table:4	\N	\N	\N
29	inserted_into_table:4	\N	\N	\N
30	inserted_into_table:4	\N	\N	\N
31	inlined_delete:3	\N	\N	\N
32	inlined_insert:3	\N	\N	\N
33	inlined_insert:3,inlined_delete:3	\N	\N	\N
34	inlined_delete:3	\N	\N	\N
35	created_table:"main"."inlined_types"	\N	\N	\N
36	inlined_insert:6	\N	\N	\N
37	created_table:"main"."inlined_nested"	\N	\N	\N
38	inlined_insert:7	\N	\N	\N
39	created_table:"main"."inlined_evolved"	\N	\N	\N
40	inlined_insert:8	\N	\N	\N
41	altered_table:8	\N	\N	\N
42	inlined_insert:8	\N	\N	\N
43	altered_table:8	\N	\N	\N
44	inlined_insert:8	\N	\N	\N
\.


--
-- Data for Name: ducklake_sort_expression; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_sort_expression (sort_id, table_id, sort_key_index, expression, dialect, sort_direction, null_order) FROM stdin;
\.


--
-- Data for Name: ducklake_sort_info; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_sort_info (sort_id, table_id, begin_snapshot, end_snapshot) FROM stdin;
\.


--
-- Data for Name: ducklake_table; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) FROM stdin;
1	019f75dd-c7df-7521-affc-e71cb986343b	1	\N	0	partitioned	partitioned/	t
3	019f75dd-c879-742b-832b-379131c48215	9	\N	0	inlined_mixed	inlined_mixed/	t
4	019f75dd-c8b1-76f7-bba2-293b461d6320	11	\N	0	partitioned_cal	partitioned_cal/	t
6	019f75dd-ca92-7a29-9abf-e4fb18f1ca6e	35	\N	0	inlined_types	inlined_types/	t
7	019f75dd-cadc-7fc3-829d-e2dcd345d070	37	\N	0	inlined_nested	inlined_nested/	t
8	019f75dd-cb28-7acd-bd36-db22ba7e824c	39	\N	0	inlined_evolved	inlined_evolved/	t
\.


--
-- Data for Name: ducklake_table_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) FROM stdin;
8	1	f	\N	1	5	\N
8	2	f	\N	five	two	\N
1	1	f	\N	a	c	\N
1	2	f	\N	2023-01-01	2024-01-20	\N
1	3	f	\N	0	119	\N
1	4	f	\N	v0	v99	\N
4	1	f	\N	2023-01-01 12:00:00+00	2024-07-15 12:00:00+00	\N
4	2	f	\N	20230101	20240715	\N
4	3	f	\N	v230101	v240715	\N
3	1	f	\N	0	1004	\N
3	2	f	\N	file0	updated	\N
6	1	f	\N	false	true	\N
6	2	f	\N	-1	1	\N
6	3	f	\N	-2	2	\N
6	4	f	\N	-3	3	\N
6	5	f	\N	-4	4	\N
6	6	f	\N	-12345	12345	\N
6	7	f	\N	1	5	\N
6	8	f	\N	2	6	\N
6	9	f	\N	3	7	\N
6	10	f	\N	4	8	\N
6	11	f	\N	-1.5	1.5	\N
6	12	f	\N	-2.5	2.5	\N
6	13	f	\N	-12.34	12.34	\N
6	14	f	\N	str	weird 'quote	\N
6	15	f	\N	\\x00ff	blobdata	\N
6	16	f	\N	2024-01-15	2025-02-16	\N
6	17	f	\N	10:30:00	11:31:01	\N
6	18	f	\N	2024-01-15 10:30:00	2025-02-16 11:31:01.123456	\N
6	19	f	\N	2024-01-15 10:30:00+00	2025-02-16 11:31:01.123456+00	\N
6	20	f	\N	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11	b1ffbc99-9c0b-4ef8-bb6d-6bb9bd380a22	\N
7	1	f	\N	1	3	\N
7	3	t	\N	1	2	\N
7	4	t	\N	u	v w	\N
7	6	f	\N	1	3	\N
7	8	f	\N	a	c	\N
7	9	f	\N	1	3	\N
\.


--
-- Data for Name: ducklake_table_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) FROM stdin;
1	120	120	4935
4	36	36	8496
3	107	105	1128
6	2	2	0
7	3	3	0
8	5	5	0
\.


--
-- Data for Name: ducklake_tag; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value) FROM stdin;
\.


--
-- Data for Name: ducklake_view; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_view (view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) FROM stdin;
\.


--
-- Name: ducklake_data_file ducklake_data_file_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_data_file
    ADD CONSTRAINT ducklake_data_file_pkey PRIMARY KEY (data_file_id);


--
-- Name: ducklake_delete_file ducklake_delete_file_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_delete_file
    ADD CONSTRAINT ducklake_delete_file_pkey PRIMARY KEY (delete_file_id);


--
-- Name: ducklake_schema ducklake_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_schema
    ADD CONSTRAINT ducklake_schema_pkey PRIMARY KEY (schema_id);


--
-- Name: ducklake_snapshot_changes ducklake_snapshot_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_snapshot_changes
    ADD CONSTRAINT ducklake_snapshot_changes_pkey PRIMARY KEY (snapshot_id);


--
-- Name: ducklake_snapshot ducklake_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ducklake_snapshot
    ADD CONSTRAINT ducklake_snapshot_pkey PRIMARY KEY (snapshot_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 0MgBS2FGsf7MPclRqhdJyRAFEJaEFd5d1lwFwwaADzKla8dRSjKffLJWzxQMdyf

