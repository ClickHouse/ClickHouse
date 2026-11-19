import sys

from bottle import response, route, run


@route("/.well-known/jwks.json")
def server():
    result = {
        "keys": [
            {
                "kty": "RSA",
                "alg": "RS512",
                "kid": "mykid",
                "n": "0RRsKcZ5j9UckjioG4Phvav3dkg2sXP6tQ7ug0yowAo_u2HffB-1OjKuhWTpA3E3YkMKj0RrT-tuUpmZEXqCAipEV7XcfCv3o"
                     "7Poa7HTq1ti_abVwT_KyfGjoNBBSJH4LTNAyo2J8ySKSDtpAEU52iL7s40Ra6I0vqp7_aRuPF5M4zcHzN3zarG5EfSVSG1-gT"
                     "kaRv8XJbra0IeIINmKv0F4--ww8ZxXTR6cvI-MsArUiAPwzf7s5dMR4DNRG6YNTrPA0pTOqQE9sRPd62XsfU08plYm27naOUZ"
                     "O5avIPl1YO5I6Gi4kPdTvv3WFIy-QvoKoPhPCaD6EbdBpe8BbTQ",
                "e": "AQAB"},
        ]
    }
    response.status = 200
    response.content_type = "application/json"
    return result


@route("/")
def ping():
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


run(host="0.0.0.0", port=int(sys.argv[1]))
