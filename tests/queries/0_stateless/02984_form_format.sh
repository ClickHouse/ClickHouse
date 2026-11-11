#!/bin/bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE_NAME="data.tmp"
FORM_DATA="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/${FILE_NAME}"
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
touch $FORM_DATA

# Simple tests
echo -ne "col1=42&col2=Hello%2C%20World%21" > $FORM_DATA
$CLICKHOUSE_CLIENT -q "SELECT * from file('$FORM_DATA', Form, 'col1 UInt64, col2 String')"
$CLICKHOUSE_CLIENT -q "SELECT * from file('$FORM_DATA', Form, 'col2 String')"
rm $FORM_DATA

# Schema reader test
touch $FORM_DATA
echo -ne "col1=42&col2=Hello%2C%20World%21&col3=%5B1%2C%202%2C%203%5D" > $FORM_DATA
$CLICKHOUSE_CLIENT -q "DESC file('$FORM_DATA', Form)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FORM_DATA', Form)"
rm $FORM_DATA

# Test with data-raw from request
touch $FORM_DATA
echo -ne "c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10&rt.tstart=1707076768666&rt.bstart=1707076769091&rt.blstart=1707076769056&rt.end=1707076769078&t_resp=296&t_page=116&t_done=412&t_other=boomerang%7C6%2Cboomr_fb%7C425%2Cboomr_ld%7C390%2Cboomr_lat%7C35&rt.tt=2685&rt.obo=0&pt.fcp=407&nt_nav_st=1707076768666&nt_dns_st=1707076768683&nt_dns_end=1707076768684&nt_con_st=1707076768684&nt_con_end=1707076768850&nt_req_st=1707076768850&nt_res_st=1707076768962&nt_res_end=1707076768962&nt_domloading=1707076769040&nt_domint=1707076769066&nt_domcontloaded_st=1707076769067&nt_domcontloaded_end=1707076769068&nt_domcomp=1707076769069&nt_load_st=1707076769069&nt_load_end=1707076769078&nt_unload_st=1707076769040&nt_unload_end=1707076769041&nt_ssl_st=1707076768788&nt_enc_size=3209&nt_dec_size=10093&nt_trn_size=3940&nt_protocol=h2&nt_red_cnt=0&nt_nav_type=1&restiming=%7B%22https%3A%2F%2Fwww.basicrum.com%2F%22%3A%7B%22publications%2F%22%3A%226%2C88%2C88%2C54%2C54%2C3e%2Ci%2Ci%2Ch*12h5%2Ckb%2C5b8%22%2C%22assets%2Fjs%2F%22%3A%7B%22just-the-docs.js%22%3A%223am%2Ce%2Ce*12pc%2C_%2C8oj*20%22%2C%22boomerang-1.737.60.cutting-edge.min.js%22%3A%222au%2Cb%2Ca*1pu3%2C_%2C1m19*21*42%22%2C%22vendor%2Flunr.min.js%22%3A%223am%2Cd%2C8*16t2%2C_%2Cfym*20%22%7D%7D%7D&u=https%3A%2F%2Fwww.basicrum.com%2Fpublications%2F&r=https%3A%2F%2Fwww.basicrum.com%2Fcost-analyses%2F&v=1.737.60&sv=14&sm=p&rt.si=dd0c542f-7adf-4310-830a-6c0a3d157c90-s8cjr1&rt.ss=1707075325294&rt.sl=4&vis.st=visible&ua.plt=Linux%20x86_64&ua.vnd=&pid=8fftz949&n=1&c.t.fps=07*4*65*j*61&c.t.busy=2*4*0034&c.tti.vr=408&c.tti=408&c.b=2&c.f=60&c.f.d=2511&c.f.m=1&c.f.s=ls7xfl1h&dom.res=5&dom.doms=1&mem.lsln=0&mem.ssln=0&mem.lssz=2&mem.sssz=2&scr.xy=1920x1200&scr.bpp=24%2F24&scr.orn=0%2Flandscape-primary&cpu.cnc=16&dom.ln=114&dom.sz=10438&dom.ck=157&dom.img=0&dom.script=6&dom.script.ext=3&dom.iframe=0&dom.link=4&dom.link.css=1&sb=1" > $FORM_DATA
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FORM_DATA', Form) FORMAT Vertical"

rm $FORM_DATA
