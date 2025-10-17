#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

# JWK genarated by python3
#
# from uuid import uuid4
# from jwcrypto import jwk
# key = jwk.JWK.generate(alg="RS256", kty="RSA", size=2048, use="sig", kid=str(uuid4()))
# print(key.export_public())
# print(key.export_private())
#

# JWT created by python3
#
# from jwcrypto import jwt, jwk
# private_jwk = # JWK_PRIVATE
# key = jwk.JWK.from_json(private_jwk)
# token = jwt.JWT(
#     header={ "kid": key.kid, "typ": "JWT", "alg": "RS256" },
#     claims={ "iss": "clickhouse", "sub": "default" },
# )
# token.make_signed_token(key=key)
# print(token.serialize())
#

JWK_PUBLIC='{"alg":"RS256","e":"AQAB","kid":"d4bcf948-f095-4156-a46a-3c503c1921d9","kty":"RSA","n":"0z4JomMwsFSdGCuewIOZhbd_hVByHTk_YKn3cLvr4hVsmafnJsR_-VKQcwXKO3rg84rWFmyr00H4bpqNlquqe3VU7lj_mOoRRrwyUeobho-mtPPyvoGSvQ3PUBI1V3-WP0zQFJ5Pwrgxw2kf5YeFQaqM8l6m71Gm5yJ6thymiCD1sBqwOQFBGdzBGRgIWq2A11KOgNjO1IOXeOWWJNbGSdiWVnuRlyCrpQEXLc747hWfWH6xC0jdrS0Q4VLCjhPIvje8XfwrF4WCmZHzVJG-7UO-zeJFIy9gG2weA_TkBzqwCivjQKMvDNndVCDlWNTmIZ1tybdPVQd639cTi1M9oQ","use":"sig"}'
JWK_PRIVATE='{"alg":"RS256","d":"UzooGR4KiVCGxqTDWtnoETVoF775m0i2oKp0vvrw79w1wFRm0j7Bbky_ATMeor8ceK-4NkrCvVYnhl02IIFF9VRZJwy8ZkG0UJioP8YG_jRtrEcltwxlHmex3QeBtmsQ211AB7x2Hthpd48qz9CzRoFVGwXVfmrVYXzAviKzynVXPUCcyqWKvrQ21Y9k4FwG4rd61iw2gF3R09oHJmUx4Q5sQhOU3gmkFShvrLLQrEx_3CFBynHRdeqR2Liln95K58JZ_qoxhFaY0_mwuiED47Ggm_F2j8Qa8gNckt-1ziJsRG89oRqRQd0u4hlboAz5rlHM2Gyw64cSfx1-H4WB","dp":"N2c5UNAZz7a3pAbgs6-xquuaYQgeY84IStRa2ICeurgxsQIvNJmo6lNT-mwNCHW64pPd3ObtvDagf7jaTx71WH2b_ExNDpNIqTtx9YcNbXz9B7cOUIlEHCTwEd3jKZVEJ-KSeumVhb7hFZtI3y0rn2nEBqzGr90WjGLgiEC-tE0","dq":"xJY-NCRoJa8anJN3iIl9cx4rpYV6PEMbKsBR7Hma4YTOYQPyRHQzJTjI-T8oz6ksoIQS5ouH_0xdWYPqaAReqeIEG6oVsR1tp42XrMdUfqm85t3Hb9V7z-Q1D6ulQscrJMayCiiChaGOOBI96nBJepzMkyqcOTWFQ3I7IwUuBEU","e":"AQAB","kid":"d4bcf948-f095-4156-a46a-3c503c1921d9","kty":"RSA","n":"0z4JomMwsFSdGCuewIOZhbd_hVByHTk_YKn3cLvr4hVsmafnJsR_-VKQcwXKO3rg84rWFmyr00H4bpqNlquqe3VU7lj_mOoRRrwyUeobho-mtPPyvoGSvQ3PUBI1V3-WP0zQFJ5Pwrgxw2kf5YeFQaqM8l6m71Gm5yJ6thymiCD1sBqwOQFBGdzBGRgIWq2A11KOgNjO1IOXeOWWJNbGSdiWVnuRlyCrpQEXLc747hWfWH6xC0jdrS0Q4VLCjhPIvje8XfwrF4WCmZHzVJG-7UO-zeJFIy9gG2weA_TkBzqwCivjQKMvDNndVCDlWNTmIZ1tybdPVQd639cTi1M9oQ","p":"9uL19P0jhhfEKzZfrUpVKgZBE8Pm7tydKZiHcFnOGdj7O-3UxoVTo4o7UU6d0V0G_EVJRzCO7b8ntfDSaVUb_2y_b-CxCol28uUF4gsWV5j_Dgx2tNkbeZGuIgv1eiZt7QEsL8PZn7-BW4NpTQ9iTj_xPy7Lomr0K_WSbV74V4U","q":"2wo-lpfsgrk15vutuTWZVMBZtYBqIomKPhsH65ySjEqz-hu9GP9bxDdNNwS4-Bd8iDJCGyApaWbuYrUv93-45ISZI864Vdd0_Z2x3lWvhD8owOg7tCJUZdPLCk4X-XkuhZk8eIBK2iFzjqZYoA28UP-H4BpAKU1zrRynaeXmMm0","qi":"WFbp7GY-SObgl76xG5u38UW6JYWio2EN6fATI0S5q36cm1PZkaW137fVyo5EceEFVFp1RnFbB5vaOnFPKmyYH_6fyAJ1ReAJ-8H04mV951yb-uOfSNcZzsM41CQPv53xcXPTQ-o-fu5njH23dREb6URY9_wAFvSQtWW_m9v5DxY","use":"sig"}'
JWKS="{\"keys\":[$JWK_PUBLIC]}"
JWT='eyJhbGciOiJSUzI1NiIsImtpZCI6ImQ0YmNmOTQ4LWYwOTUtNDE1Ni1hNDZhLTNjNTAzYzE5MjFkOSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbGlja2hvdXNlIiwic3ViIjoiZGVmYXVsdCJ9.ZmCyKmfuje1m2uzsWGHv4nbyVLq5PRr1ydPNq3gl5B_beyuaCkWNZCuXQcQyuCMyljTT1BPzVqNA2Yq-prEe_aL5wgbuDfrGGSHzHawnmjDS_rtAnKO2H9faZuv8Oc7JZQ_XQt1pMyQg2gGL5R1GE_h8Flm0AClJPTuMRCEo6Q0QJwjIAhPhSkRajucI7q6BHnrPXSgKAsiCW9eq-9zR50Q_GKxBNW-sn54Mw-EqGilneyhdhghBohWKlEbLqNMiecVYZmn7BzjCPH-j6J7kofCU7Di7ysIHRQllWuxs89WvF7NEiKzHfVdx8PhjFbc0sxdBLS41YAGCmgXAaJccpg'
JWT_2='eyJhbGciOiJSUzI1NiIsImtpZCI6ImQ0YmNmOTQ4LWYwOTUtNDE1Ni1hNDZhLTNjNTAzYzE5MjFkOSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbGlja2hvdXNlIiwic3ViIjoidXNlcl9qd3QifQ.gi8EwXx07f8Mp5SaOrklZnOV9mid-WDzwmjc1KCAVcRzHj08wrkEccX55RSrlmeKPlInInTck7R5GVw7mw375bQkvMcMvcyfd_7zhkfSgFN1gh266jIJHA2ELKqWdJ8pTaPfgFnzbcObzPyvD35Wi2jzMQ7m1vBhqGt59906d4GAGOUEnBPXqfQrtF-Z-D0s-zu4-ChaHbg7MeDGvbIrftIif36D_8U_Ylm2KTNzsTyI0AIN7fF-jCzsP__aIcje21sENpvcD7otQvLAY-JfALZ4c71_DdIdstzxez7YPcW7XML_MfMSl4MGpiZG4LhyU6jWVto_FzBhkWoDC0-BAQ'

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONFIG_FILE=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml
USERS_FILE=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).users.xml

cp $USERS_FILE $CURDIR/users.xml
sed -i "s|<jwks\/>|<jwks>$JWKS<\/jwks>|g" $CURDIR/users.xml

export CLICKHOUSE_CURL_TIMEOUT=2

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY --config-file=$CONFIG_FILE &> $CURDIR/clickhouse-server.stderr &
server_pid=$!

CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:3642"

# wait for the server to start accepting tcp connections (max 30 seconds)
i=0 retries=30
while [[ $i -lt $retries ]] && ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; do
    sleep 1
    ((++i))
done
if ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; then
    exit 1
fi

echo "SELECT 42" | ${CLICKHOUSE_CURL} -sS -H "Authorization: Bearer $JWT" "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT 42" | ${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" --data-binary @- | grep -c 'X-ClickHouse-Exception-Code: 194'
echo "SELECT 42" | ${CLICKHOUSE_CURL} -sS -i -H "Authorization: never" "${CLICKHOUSE_URL}" --data-binary @- | grep -c 'X-ClickHouse-Exception-Code: 516'

${CLICKHOUSE_CURL} -sS -H "Authorization: Bearer $JWT" "${CLICKHOUSE_URL}" -d "SELECT 42"
${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -d "SELECT 42" | grep -c 'X-ClickHouse-Exception-Code: 194'
${CLICKHOUSE_CURL} -sS -i -H "Authorization: never" "${CLICKHOUSE_URL}" -d "SELECT 42" | grep -c 'X-ClickHouse-Exception-Code: 516'

echo "SELECT 42" | ${CLICKHOUSE_CURL} -sS -H "Authorization: Bearer $JWT_2" "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CURL} -sS -H "Authorization: Bearer $JWT_2" "${CLICKHOUSE_URL}" -d "SELECT 42"

kill $server_pid
wait $server_pid
return_code=$?

exit $return_code
