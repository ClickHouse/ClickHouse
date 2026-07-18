--
-- PostgreSQL database dump
--

\restrict rSH1XwpmuJaTVXRmDMExahVeEyAFyXslVDZPMhbyxNXEDURCbafbpYnLtG8EPuM

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
-- Name: ducklake_inlined_data_tables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ducklake_inlined_data_tables (
    table_id bigint,
    table_name character varying,
    schema_version bigint
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
1	1	\N	1	1	id	int32	\N	NULL	t	\N	literal	duckdb
3	1	\N	1	3	region	varchar	\N	NULL	t	\N	literal	duckdb
4	1	\N	1	4	s	struct	\N	NULL	t	\N		duckdb
5	1	\N	1	5	x	int32	\N	NULL	t	4	literal	duckdb
6	1	\N	1	6	y	varchar	\N	NULL	t	4	literal	duckdb
7	1	\N	1	7	l	list	\N	NULL	t	\N		duckdb
8	1	\N	1	8	element	int32	\N	NULL	t	7	literal	duckdb
9	1	\N	1	9	m	map	\N	NULL	t	\N		duckdb
10	1	\N	1	10	key	varchar	\N	NULL	t	9	literal	duckdb
11	1	\N	1	11	value	int32	\N	NULL	t	9	literal	duckdb
2	1	5	1	2	name	varchar	\N	NULL	t	\N	literal	duckdb
2	5	\N	1	2	title	varchar	\N	NULL	t	\N	literal	duckdb
12	6	\N	1	12	extra	float64	\N	NULL	t	\N	literal	duckdb
\.


--
-- Data for Name: ducklake_column_mapping; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_column_mapping (mapping_id, table_id, type) FROM stdin;
1	1	map_by_name
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
0	1	3	\N	\N	region=aa/ducklake-019f7651-fc3f-7076-a6c0-3dca9750cbd1.parquet	t	parquet	2	1052	746	0	2	\N	\N	\N
2	1	4	\N	\N	region=bb/ext1.parquet	t	parquet	2	2249	1679	2	2	\N	1	\N
3	1	7	\N	\N	region=cc/ducklake-019f7651-fcb9-782c-a3ce-0581bb555d2b.parquet	t	parquet	1	1148	845	4	2	\N	\N	\N
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
0	1	1	33	2	0	1	2	\N	\N
0	1	2	39	2	0	one	two	\N	\N
0	1	3	37	2	0	aa	aa	\N	\N
0	1	5	33	2	0	1	2	\N	\N
0	1	6	35	2	0	u	v	\N	\N
0	1	8	39	2	0	1	2	\N	\N
0	1	10	39	2	0	a	b	\N	\N
0	1	11	39	2	0	1	2	\N	\N
2	1	1	82	2	0	3	4	\N	\N
2	1	2	80	2	0	four	three	\N	\N
2	1	3	0	2	0	bb	bb	\N	\N
2	1	5	82	2	0	3	4	\N	\N
2	1	6	66	2	0	w	x	\N	\N
2	1	8	88	2	0	3	4	\N	\N
2	1	10	72	2	0	c	d	\N	\N
2	1	11	88	2	0	3	4	\N	\N
3	1	1	29	1	0	5	5	\N	\N
3	1	2	33	1	0	five	five	\N	\N
3	1	3	31	1	0	cc	cc	\N	\N
3	1	5	29	1	0	5	5	\N	\N
3	1	6	30	1	0	z	z	\N	\N
3	1	8	35	1	0	5	5	\N	\N
3	1	10	36	1	0	e	e	\N	\N
3	1	11	35	1	0	5	5	\N	\N
3	1	12	33	1	0	5.5	5.5	f	\N
\.


--
-- Data for Name: ducklake_file_partition_value; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value) FROM stdin;
0	1	0	aa
2	1	0	bb
3	1	0	cc
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
-- Data for Name: ducklake_inlined_data_tables; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_inlined_data_tables (table_id, table_name, schema_version) FROM stdin;
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
data_path	/var/lib/clickhouse/user_files/ducklake_data3/	\N	\N
\.


--
-- Data for Name: ducklake_name_mapping; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_name_mapping (mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) FROM stdin;
1	0	id	1	\N	f
1	1	name	2	\N	f
1	2	s	4	\N	f
1	3	x	5	2	f
1	4	y	6	2	f
1	5	l	7	\N	f
1	6	list	8	5	f
1	7	m	9	\N	f
1	8	key	10	7	f
1	9	value	11	7	f
1	10	region	3	\N	t
\.


--
-- Data for Name: ducklake_partition_column; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) FROM stdin;
2	1	0	3	identity
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
0	78c16d22-fa79-4516-bb23-927d694227a0	0	\N	main	main/	t
\.


--
-- Data for Name: ducklake_schema_versions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_schema_versions (begin_snapshot, schema_version, table_id) FROM stdin;
1	1	1
2	2	1
5	3	1
6	4	1
\.


--
-- Data for Name: ducklake_snapshot; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) FROM stdin;
0	2026-07-18 17:41:51.996212+00	0	1	0
1	2026-07-18 17:41:52.18777+00	1	2	0
2	2026-07-18 17:41:52.238955+00	2	3	0
3	2026-07-18 17:41:52.296214+00	2	3	1
4	2026-07-18 17:41:52.326825+00	2	3	3
5	2026-07-18 17:41:52.339686+00	3	3	3
6	2026-07-18 17:41:52.366049+00	4	3	3
7	2026-07-18 17:41:52.417477+00	4	3	4
\.


--
-- Data for Name: ducklake_snapshot_changes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message, commit_extra_info) FROM stdin;
0	created_schema:"main"	\N	\N	\N
1	created_table:"main"."mapped"	\N	\N	\N
2	altered_table:1	\N	\N	\N
3	inserted_into_table:1	\N	\N	\N
4	inserted_into_table:1	\N	\N	\N
5	altered_table:1	\N	\N	\N
6	altered_table:1	\N	\N	\N
7	inserted_into_table:1	\N	\N	\N
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
1	019f7651-fbd8-7a31-be95-2deffcdff638	1	\N	0	mapped	mapped/	t
\.


--
-- Data for Name: ducklake_table_column_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) FROM stdin;
1	1	f	\N	1	5	\N
1	2	f	\N	five	two	\N
1	3	f	\N	aa	cc	\N
1	5	f	\N	1	5	\N
1	6	f	\N	u	z	\N
1	8	f	\N	1	5	\N
1	10	f	\N	a	e	\N
1	11	f	\N	1	5	\N
\.


--
-- Data for Name: ducklake_table_stats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) FROM stdin;
1	5	5	4449
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

\unrestrict rSH1XwpmuJaTVXRmDMExahVeEyAFyXslVDZPMhbyxNXEDURCbafbpYnLtG8EPuM

