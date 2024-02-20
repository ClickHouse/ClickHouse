#!/bin/bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test setup 
# USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
USER_FILES_PATH="/home/shaun/Desktop/ClickHouse/programs/server/user_files"
FILE_NAME="data.tmp"
FORM_DATA="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/${FILE_NAME}"
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
touch $FORM_DATA 

echo -ne 'c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10&rt.tstart=1707076768666&rt.bstart=1707076769091&rt.blstart=1707076769056&rt.end=1707076769078&t_resp=296&t_page=116&t_done=412&t_other=boomerang%7C6%2Cboomr_fb%7C425%2Cboomr_ld%7C390%2Cboomr_lat%7C35&rt.tt=2685&rt.obo=0&pt.fcp=407&nt_nav_st=1707076768666&nt_dns_st=1707076768683&nt_dns_end=1707076768684&nt_con_st=1707076768684&nt_con_end=1707076768850&nt_req_st=1707076768850&nt_res_st=1707076768962&nt_res_end=1707076768962&nt_domloading=1707076769040&nt_domint=1707076769066&nt_domcontloaded_st=1707076769067&nt_domcontloaded_end=1707076769068&nt_domcomp=1707076769069&nt_load_st=1707076769069&nt_load_end=1707076769078&nt_unload_st=1707076769040&nt_unload_end=1707076769041&nt_ssl_st=1707076768788&nt_enc_size=3209&nt_dec_size=10093&nt_trn_size=3940&nt_protocol=h2&nt_red_cnt=0&nt_nav_type=1&restiming=%7B%22https%3A%2F%2Fwww.basicrum.com%2F%22%3A%7B%22publications%2F%22%3A%226%2C88%2C88%2C54%2C54%2C3e%2Ci%2Ci%2Ch*12h5%2Ckb%2C5b8%22%2C%22assets%2Fjs%2F%22%3A%7B%22just-the-docs.js%22%3A%223am%2Ce%2Ce*12pc%2C_%2C8oj*20%22%2C%22boomerang-1.737.60.cutting-edge.min.js%22%3A%222au%2Cb%2Ca*1pu3%2C_%2C1m19*21*42%22%2C%22vendor%2Flunr.min.js%22%3A%223am%2Cd%2C8*16t2%2C_%2Cfym*20%22%7D%7D%7D&u=https%3A%2F%2Fwww.basicrum.com%2Fpublications%2F&r=https%3A%2F%2Fwww.basicrum.com%2Fcost-analyses%2F&v=1.737.60&sv=14&sm=p&rt.si=dd0c542f-7adf-4310-830a-6c0a3d157c90-s8cjr1&rt.ss=1707075325294&rt.sl=4&vis.st=visible&ua.plt=Linux%20x86_64&ua.vnd=&pid=8fftz949&n=1&c.t.fps=07*4*65*j*61&c.t.busy=2*4*0034&c.tti.vr=408&c.tti=408&c.b=2&c.f=60&c.f.d=2511&c.f.m=1&c.f.s=ls7xfl1h&dom.res=5&dom.doms=1&mem.lsln=0&mem.ssln=0&mem.lssz=2&mem.sssz=2&scr.xy=1920x1200&scr.bpp=24%2F24&scr.orn=0%2Flandscape-primary&cpu.cnc=16&dom.ln=114&dom.sz=10438&dom.ck=157&dom.img=0&dom.script=6&dom.script.ext=3&dom.iframe=0&dom.link=4&dom.link.css=1&sb=1' > $FORM_DATA

$CLICKHOUSE_CLIENT -nm --query "
DROP TABLE IF EXISTS form_data;
CREATE TABLE IF NOT EXISTS form_data 
(
c Nested
(
    t Nested
    (
        fps Nullable(String),
        busy Nullable(String)
    ),
    tti Nested
    (
        tti_value Nullable(Int64),
        vr Nullable(Int64),
        m Nullable(String)
    ),
    f Nested
    (
        f_value Nullable(Int64),
        d Nullable(Int64),
        m Nullable(Int64),
        s Nullable(String)
    ),
    e Nullable(String),
    b Nullable(Int64) 
),
rt Nested
(
    start Nullable(String),
    bmr Nullable(String),
    tstart Nullable(Int64), 
    bstart Nullable(Int64), 
    blstart Nullable(Int64), 
    end Nullable(Int64),
    tt Nullable(Int64), 
    obo Nullable(Int64),
    si Nullable(String),
    ss Nullable(Int64), 
    sl Nullable(Int64)   
),
pt Nested
(
    fcp Nullable(Int64)
),
vis Nested
(
    st Nullable(String)
),
ua Nested
(
    plt Nullable(String),
    vnd Nullable(String)
),
dom Nested
(
    res Nullable(Int64), 
    doms Nullable(Int64),
    ln Nullable(Int64), 
    sz Nullable(Int64), 
    ck Nullable(Int64), 
    img Nullable(Int64), 
    iframe Nullable(Int64), 
    link Nested
    (
        link_value Nullable(Int64),
        css Nullable(Int64)
    ),
    script Nested
    (
        script_value Nullable(Int64),
        ext Nullable(Int64)
    ) 
),
mem Nested
(
    lsln Nullable(Int64), 
    ssln Nullable(Int64), 
    lssz Nullable(Int64), 
    sssz Nullable(Int64) 
),
scr Nested
(
    xy Nullable(String),
    bpp Nullable(String),
    orn Nullable(String)
),
cpc Nested
(
    cnc Nullable(Int64)
),
t_resp Nullable(Int64), 
t_page Nullable(Int64), 
t_done Nullable(Int64), 
t_other Nullable(String),
nt_nav_st Nullable(Int64), 
nt_dns_st Nullable(Int64), 
nt_dns_end Nullable(Int64), 
nt_con_st Nullable(Int64), 
nt_con_end Nullable(Int64), 
nt_req_st Nullable(Int64), 
nt_res_st Nullable(Int64), 
nt_res_end Nullable(Int64), 
nt_domloading Nullable(Int64), 
nt_domint Nullable(Int64), 
nt_domcontloaded_st Nullable(Int64), 
nt_domcontloaded_end Nullable(Int64), 
nt_domcomp Nullable(Int64), 
nt_load_st Nullable(Int64), 
nt_load_end Nullable(Int64), 
nt_unload_st Nullable(Int64) ,
nt_unload_end Nullable(Int64), 
nt_ssl_st Nullable(Int64), 
nt_enc_size Nullable(Int64), 
nt_dec_size Nullable(Int64), 
nt_trn_size Nullable(Int64), 
nt_protocol Nullable(String),
nt_red_cnt Nullable(Int64), 
nt_nav_type Nullable(Int64), 
restiming Nullable(String),
u Nullable(String),
r Nullable(String),
v Nullable(String),
sv Nullable(Int64), 
sm Nullable(String),
sb Nullable(Int64),
pid Nullable(String),
n Nullable(Int64)  
) 
ENGINE = TinyLog;
"

# Insert Form data from clickhouse-client
$CLICKHOUSE_CLIENT --query "INSERT INTO default.form_data FORMAT Form" < $FORM_DATA
$CLICKHOUSE_CLIENT --query "SELECT * FROM default.form_data FORMAT Vertical"

#${CLICKHOUSE_CURL} http://localhost:8123/?query=INSERT%20INTO%20form%5Fdata%20FORMAT%20Form --data-binary @- < $FORM_DATA

# Test teardown
rm -r ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}