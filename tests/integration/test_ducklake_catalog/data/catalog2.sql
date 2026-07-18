--
-- PostgreSQL database dump
--

\restrict aqAsPgWoXMKraw1bclChL3h4gUHkNcLuRJ6hKVBkuRklbbbktjHCEtvNc0YFxNi

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
-- Name: ducklake_inlined_data_3_4; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_3_4 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    v bytea
);


--
-- Name: ducklake_inlined_data_4_5; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_4_5 (
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
-- Name: ducklake_inlined_data_5_6; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_5_6 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    s character varying,
    l character varying,
    m character varying
);


--
-- Name: ducklake_inlined_data_6_7; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_6_7 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a bytea
);


--
-- Name: ducklake_inlined_data_6_8; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_6_8 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a bytea,
    b double precision
);


--
-- Name: ducklake_inlined_data_6_9; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_6_9 (
    row_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    id integer,
    a2 bytea,
    b double precision
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
1	15	\N	4	1	b	boolean	\N	NULL	t	\N	literal	duckdb
2	15	\N	4	2	i8	int8	\N	NULL	t	\N	literal	duckdb
3	15	\N	4	3	i16	int16	\N	NULL	t	\N	literal	duckdb
4	15	\N	4	4	i32	int32	\N	NULL	t	\N	literal	duckdb
5	15	\N	4	5	i64	int64	\N	NULL	t	\N	literal	duckdb
6	15	\N	4	6	h	int128	\N	NULL	t	\N	literal	duckdb
7	15	\N	4	7	u8	uint8	\N	NULL	t	\N	literal	duckdb
8	15	\N	4	8	u16	uint16	\N	NULL	t	\N	literal	duckdb
9	15	\N	4	9	u32	uint32	\N	NULL	t	\N	literal	duckdb
10	15	\N	4	10	u64	uint64	\N	NULL	t	\N	literal	duckdb
11	15	\N	4	11	f32	float32	\N	NULL	t	\N	literal	duckdb
12	15	\N	4	12	f64	float64	\N	NULL	t	\N	literal	duckdb
13	15	\N	4	13	d	decimal(10,2)	\N	NULL	t	\N	literal	duckdb
14	15	\N	4	14	vc	varchar	\N	NULL	t	\N	literal	duckdb
15	15	\N	4	15	bl	blob	\N	NULL	t	\N	literal	duckdb
16	15	\N	4	16	dt	date	\N	NULL	t	\N	literal	duckdb
17	15	\N	4	17	tm	time	\N	NULL	t	\N	literal	duckdb
18	15	\N	4	18	ts	timestamp	\N	NULL	t	\N	literal	duckdb
19	15	\N	4	19	tstz	timestamptz	\N	NULL	t	\N	literal	duckdb
20	15	\N	4	20	u	uuid	\N	NULL	t	\N	literal	duckdb
1	17	\N	5	1	id	int32	\N	NULL	t	\N	literal	duckdb
2	17	\N	5	2	s	struct	\N	NULL	t	\N		duckdb
3	17	\N	5	3	x	int32	\N	NULL	t	2	literal	duckdb
4	17	\N	5	4	y	varchar	\N	NULL	t	2	literal	duckdb
5	17	\N	5	5	l	list	\N	NULL	t	\N		duckdb
6	17	\N	5	6	element	int32	\N	NULL	t	5	literal	duckdb
7	17	\N	5	7	m	map	\N	NULL	t	\N		duckdb
8	17	\N	5	8	key	varchar	\N	NULL	t	7	literal	duckdb
9	17	\N	5	9	value	int32	\N	NULL	t	7	literal	duckdb
1	19	\N	6	1	id	int32	\N	NULL	t	\N	literal	duckdb
3	21	\N	6	3	b	float64	\N	NULL	t	\N	literal	duckdb
2	19	23	6	2	a	varchar	\N	NULL	t	\N	literal	duckdb
2	23	\N	6	2	a2	varchar	\N	NULL	t	\N	literal	duckdb
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
0	1	3	\N	\N	region=a/year=2023/ducklake-019f72c7-c431-7b2f-863a-dcf1e4a47848.parquet	t	parquet	20	815	377	0	2	\N	\N	\N
1	1	4	\N	\N	region=a/year=2024/ducklake-019f72c7-c441-7ed6-b23a-4e5e52358718.parquet	t	parquet	20	824	381	20	2	\N	\N	\N
2	1	5	\N	\N	region=b/year=2023/ducklake-019f72c7-c44f-7c7a-a26d-00260d4ca414.parquet	t	parquet	20	824	381	40	2	\N	\N	\N
3	1	6	\N	\N	region=b/year=2024/ducklake-019f72c7-c45c-7098-b220-5e12d3961fa6.parquet	t	parquet	20	824	381	60	2	\N	\N	\N
4	1	7	\N	\N	region=c/year=2023/ducklake-019f72c7-c46a-7974-a725-3537a2f750e2.parquet	t	parquet	20	824	381	80	2	\N	\N	\N
5	1	8	\N	\N	region=c/year=2024/ducklake-019f72c7-c477-7a62-9e8b-482ed1ec7d1d.parquet	t	parquet	20	824	385	100	2	\N	\N	\N
6	3	10	\N	\N	ducklake-019f72c7-c4b8-7708-833e-60897950f7c1.parquet	t	parquet	100	1128	241	0	\N	\N	\N	\N
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
-- Data for Name: ducklake_inlined_data_3_4; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_3_4 (row_id, begin_snapshot, end_snapshot, id, v) FROM stdin;
100	12	\N	1000	\\x696e6c30
104	12	\N	1004	\\x696e6c34
101	13	\N	1001	\\x75706461746564
103	13	\N	1003	\\x75706461746564
101	12	13	1001	\\x696e6c31
103	12	13	1003	\\x696e6c33
102	12	14	1002	\\x696e6c32
\.


--
-- Data for Name: ducklake_inlined_data_4_5; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_4_5 (row_id, begin_snapshot, end_snapshot, b, i8, i16, i32, i64, h, u8, u16, u32, u64, f32, f64, d, vc, bl, dt, tm, ts, tstz, u) FROM stdin;
0	16	\N	t	-1	-2	-3	-4	12345	1	2	3	4	1.5	2.5	12.34	\\x737472	\\x626c6f6264617461	2024-01-15	10:30:00	2024-01-15 10:30:00	2024-01-15 10:30:00+00	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
1	16	\N	f	1	2	3	4	-12345	5	6	7	8	-1.5	-2.5	-12.34	\\x7765697264202771756f7465	\\x006666	2025-02-16	11:31:01	2025-02-16 11:31:01.123456	2025-02-16 11:31:01.123456+00	b1ffbc99-9c0b-4ef8-bb6d-6bb9bd380a22
\.


--
-- Data for Name: ducklake_inlined_data_5_6; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_5_6 (row_id, begin_snapshot, end_snapshot, id, s, l, m) FROM stdin;
0	18	\N	1	{'x': 1, 'y': 'u'}	[1, 2]	{a=1}
1	18	\N	2	{'x': 2, 'y': 'v w'}	[3]	{b=2, c=3}
2	18	\N	3	\N	\N	\N
\.


--
-- Data for Name: ducklake_inlined_data_6_7; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_6_7 (row_id, begin_snapshot, end_snapshot, id, a) FROM stdin;
0	20	\N	1	\\x6f6e65
1	20	\N	2	\\x74776f
\.


--
-- Data for Name: ducklake_inlined_data_6_8; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_6_8 (row_id, begin_snapshot, end_snapshot, id, a, b) FROM stdin;
2	22	\N	3	\\x7468726565	3.5
3	22	\N	4	\\x666f7572	4.5
\.


--
-- Data for Name: ducklake_inlined_data_6_9; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_6_9 (row_id, begin_snapshot, end_snapshot, id, a2, b) FROM stdin;
4	24	\N	5	\\x66697665	5.5
\.


--
-- Data for Name: ducklake_inlined_data_tables; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_tables (table_id, table_name, schema_version) FROM stdin;
3	ducklake_inlined_data_3_4	4
4	ducklake_inlined_data_4_5	5
5	ducklake_inlined_data_5_6	6
6	ducklake_inlined_data_6_7	7
6	ducklake_inlined_data_6_8	8
6	ducklake_inlined_data_6_9	9
\.


--
-- Data for Name: ducklake_inlined_delete_3; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_delete_3 (file_id, row_id, begin_snapshot) FROM stdin;
6	3	11
6	17	11
6	42	11
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
\.


--
-- Data for Name: ducklake_partition_info; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot) FROM stdin;
2	1	2	\N
\.


--
-- Data for Name: ducklake_schema; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) FROM stdin;
0	7024eb66-5463-42cf-bea9-74867cfbfc98	0	\N	main	main/	t
\.


--
-- Data for Name: ducklake_schema_versions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_schema_versions (begin_snapshot, schema_version, table_id) FROM stdin;
1	1	1
2	2	1
9	3	3
15	5	4
17	6	5
19	7	6
21	8	6
23	9	6
\.


--
-- Data for Name: ducklake_snapshot; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) FROM stdin;
0	2026-07-18 01:12:02.02106+00	0	1	0
1	2026-07-18 01:12:02.234669+00	1	2	0
2	2026-07-18 01:12:02.275239+00	2	3	0
3	2026-07-18 01:12:02.329403+00	2	3	1
4	2026-07-18 01:12:02.36604+00	2	3	2
5	2026-07-18 01:12:02.381259+00	2	3	3
6	2026-07-18 01:12:02.393494+00	2	3	4
7	2026-07-18 01:12:02.408081+00	2	3	5
8	2026-07-18 01:12:02.420318+00	2	3	6
9	2026-07-18 01:12:02.433189+00	3	4	6
10	2026-07-18 01:12:02.462277+00	3	4	7
11	2026-07-18 01:12:02.513469+00	3	4	7
12	2026-07-18 01:12:02.561436+00	4	4	8
13	2026-07-18 01:12:02.578674+00	4	4	9
14	2026-07-18 01:12:02.637521+00	4	4	9
15	2026-07-18 01:12:02.660196+00	5	5	9
16	2026-07-18 01:12:02.70261+00	5	5	10
17	2026-07-18 01:12:02.74255+00	6	6	10
18	2026-07-18 01:12:02.784978+00	6	6	11
19	2026-07-18 01:12:02.821461+00	7	7	11
20	2026-07-18 01:12:02.845693+00	7	7	12
21	2026-07-18 01:12:02.878569+00	8	7	12
22	2026-07-18 01:12:02.912394+00	8	7	13
23	2026-07-18 01:12:02.946569+00	9	7	13
24	2026-07-18 01:12:02.978487+00	9	7	14
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
11	inlined_delete:3	\N	\N	\N
12	inlined_insert:3	\N	\N	\N
13	inlined_insert:3,inlined_delete:3	\N	\N	\N
14	inlined_delete:3	\N	\N	\N
15	created_table:"main"."inlined_types"	\N	\N	\N
16	inlined_insert:4	\N	\N	\N
17	created_table:"main"."inlined_nested"	\N	\N	\N
18	inlined_insert:5	\N	\N	\N
19	created_table:"main"."inlined_evolved"	\N	\N	\N
20	inlined_insert:6	\N	\N	\N
21	altered_table:6	\N	\N	\N
22	inlined_insert:6	\N	\N	\N
23	altered_table:6	\N	\N	\N
24	inlined_insert:6	\N	\N	\N
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
1	019f72c7-c3cf-7ed9-8cdb-0080ffccdf3b	1	\N	0	partitioned	partitioned/	t
3	019f72c7-c482-72fe-815a-b6d595763471	9	\N	0	inlined_mixed	inlined_mixed/	t
4	019f72c7-c566-7e39-99b7-6cc33395b73d	15	\N	0	inlined_types	inlined_types/	t
5	019f72c7-c5b9-73bd-bdef-8c4abb664737	17	\N	0	inlined_nested	inlined_nested/	t
6	019f72c7-c606-7937-9f0d-8c6fb162a8b8	19	\N	0	inlined_evolved	inlined_evolved/	t
\.


--
-- Data for Name: ducklake_table_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) FROM stdin;
1	1	f	\N	a	c	\N
1	2	f	\N	2023-01-01	2024-01-20	\N
1	3	f	\N	0	119	\N
1	4	f	\N	v0	v99	\N
3	1	f	\N	0	1004	\N
3	2	f	\N	file0	updated	\N
4	1	f	\N	false	true	\N
4	2	f	\N	-1	1	\N
4	3	f	\N	-2	2	\N
4	4	f	\N	-3	3	\N
4	5	f	\N	-4	4	\N
4	6	f	\N	-12345	12345	\N
4	7	f	\N	1	5	\N
4	8	f	\N	2	6	\N
4	9	f	\N	3	7	\N
4	10	f	\N	4	8	\N
4	11	f	\N	-1.5	1.5	\N
4	12	f	\N	-2.5	2.5	\N
4	13	f	\N	-12.34	12.34	\N
4	14	f	\N	str	weird 'quote	\N
4	15	f	\N	\\x00ff	blobdata	\N
4	16	f	\N	2024-01-15	2025-02-16	\N
4	17	f	\N	10:30:00	11:31:01	\N
4	18	f	\N	2024-01-15 10:30:00	2025-02-16 11:31:01.123456	\N
4	19	f	\N	2024-01-15 10:30:00+00	2025-02-16 11:31:01.123456+00	\N
4	20	f	\N	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11	b1ffbc99-9c0b-4ef8-bb6d-6bb9bd380a22	\N
5	1	f	\N	1	3	\N
5	3	t	\N	1	2	\N
5	4	t	\N	u	v w	\N
5	6	f	\N	1	3	\N
5	8	f	\N	a	c	\N
5	9	f	\N	1	3	\N
6	1	f	\N	1	5	\N
6	2	f	\N	five	two	\N
\.


--
-- Data for Name: ducklake_table_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) FROM stdin;
1	120	120	4935
3	107	105	1128
4	2	2	0
5	3	3	0
6	5	5	0
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

\unrestrict aqAsPgWoXMKraw1bclChL3h4gUHkNcLuRJ6hKVBkuRklbbbktjHCEtvNc0YFxNi

