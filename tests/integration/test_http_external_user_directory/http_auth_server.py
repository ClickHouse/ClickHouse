import base64
import collections
import http.server
import json
import socket
import threading

GOOD_PASSWORD = "good_password"

# Barriers all N concurrent barrier_user_* requests before replying 200 to any of them.
# Proves the http directory does not serialize remote I/O behind a directory-wide lock:
# with such a lock, the second request would never reach the mock while the first is
# blocked authenticating, the barrier would never fill, and it would time out (503).
BARRIER_PARTIES = 4
CONCURRENT_BARRIER = threading.Barrier(BARRIER_PARTIES, timeout=10)

# Usernames this server has been asked to authenticate — used by the precedence tests to
# prove the http directory actually reached the server (rather than being bypassed by a
# higher-precedence users.xml). Shared across threads; set operations are atomic enough here.
SEEN_USERS = set()

# Per-user count of authentication requests that reached this server. Used by
# `test_helper_request_counts` to prove that a received HTTP response is parsed exactly
# once (never retried), while a transport failure (no HTTP response at all) is retried
# up to `max_tries`.
REQUEST_COUNTS = collections.Counter()

# user -> dict describing the response for a correct password.
# "status" overrides the HTTP status (default 200); "body" is sent verbatim
# if it is a string, else JSON-encoded.
MAIN_USERS = {
    "http_user": {"body": {"roles": ["reader"]}},
    "norole_user": {"body": {}},
    "emptyroles_user": {"body": {"roles": []}},
    "settings_user": {"body": {"settings": {"max_threads": "4"}, "roles": ["reader"]}},
    "prefix_user": {
        "body": {"roles": ["external_team1"]}
    },  # external_ prefix delegation
    "disallowed_role_user": {"body": {"roles": ["admin_role"]}},
    "mixed_roles_user": {"body": {"roles": ["reader", "admin_role"]}},
    "unknown_role_user": {"body": {"roles": ["no_such_role"]}},
    "malformed_json_user": {"body": "{not json"},
    "bad_roles_type_user": {"body": {"roles": "reader"}},
    "expired_user": {"body": {"valid_until": 1000000000, "roles": ["reader"]}},
    "negative_vu_user": {"body": {"valid_until": -5}},
    "future_vu_user": {"body": {"valid_until": 4102444800, "roles": ["reader"]}},
    "err500_user": {"status": 500},
    "err429_user": {"status": 429},
    "local_user": {
        "body": {"roles": ["reader"]}
    },  # also exists in users.xml with another password
    "rowpolicy_user": {"body": {"roles": ["policy_role"]}},
    "profileclash_user": {"body": {"settings": {"max_threads": "7"}, "roles": []}},
    "distributed_user": {"body": {"roles": ["cluster_role"]}},
    # Mixed set: reader exists on both nodes, only_node1_role only on `node`. Proves the
    # receiver fails closed on a PARTIALLY-resolvable set, not just an all-missing one.
    "halfcluster_user": {"body": {"roles": ["reader", "only_node1_role"]}},
    # Used to prove the interserver AlwaysAllowCredentials path never contacts the
    # receiving node's own HTTP server: authenticated only by `node`'s mock.
    "interserver_user": {"body": {"roles": ["cluster_role"]}},
    "http_user_concurrent": {"body": {"roles": ["reader"]}},
    "legacy_settings_user": {"body": {"settings": {"max_threads": "4"}}},
    # Used only by test_metrics for delta-based ProfileEvents/CurrentMetrics assertions.
    "metrics_user": {"body": {}},
    # 200 with Content-Length: 0 - the "empty body means {}" contract with verifiable framing.
    "content_length_zero_user": {"body": ""},
    # A duplicate JSON member: Poco would apply the last one; the directory must reject it.
    "dup_roles_user": {"body": '{"roles": ["reader"], "roles": ["admin_role"]}'},
    # Wrong password is answered with a 401 that carries a body (see _auth_against); the
    # unread body must not poison the pooled connection for the next authentication.
    "body401_user": {"body": {}},
    # Creates a SQL SECURITY DEFINER view; external_definer is prefix-delegated.
    "definer_user": {"body": {"roles": ["external_definer"]}},
    # Custom settings under the configured `SQL_` prefix keep their JSON scalar types; the
    # built-in max_threads is sent as a JSON number to prove built-ins are cast, not parsed
    # from strings only.
    "custom_settings_user": {
        "body": {
            "settings": {
                "SQL_tenant": "acme",
                "SQL_region_id": 42,
                "SQL_feature_enabled": True,
                "max_threads": 4,
            }
        }
    },
    # A typo in a built-in name is neither built-in nor prefixed: fails closed.
    "typo_setting_user": {"body": {"settings": {"max_threds": "4"}}},
    # A custom setting outside custom_settings_prefixes: fails closed.
    "unprefixed_setting_user": {"body": {"settings": {"other_tenant": "acme"}}},
    # A built-in setting with a value of the wrong kind: fails closed at authentication.
    "bad_value_setting_user": {"body": {"settings": {"max_threads": "many"}}},
}
MAIN_USERS.update({f"barrier_user_{i}": {"body": {}} for i in range(BARRIER_PARTIES)})

# dual_user: the returned role set depends on the password, to simulate
# membership changes between authentications of the same username.
DUAL_USER_PASSWORDS = {
    "password_a": {"roles": ["role_a"]},
    "password_b": {"roles": ["role_b"]},
    "password_none": {"roles": []},
}

# probe_user: like dual_user, but with roles delegated for the named-session
# role-profile contract test (probe_role_a/probe_role_b).
PROBE_USER_PASSWORDS = {
    "password_a": {"roles": ["probe_role_a"]},
    "password_b": {"roles": ["probe_role_b"]},
}

# guard_user: used by test_failed_named_session_init_not_reusable. "cause_fail" returns a
# role/setting combination that violates that role's profile constraint (checked at
# named-session creation, AFTER acquireSession has already published the session) so the
# named-session cleanup guard is exercised; "valid" is a normal authentication
# used afterwards to prove the failed session was not left reusable.
GUARD_USER_PASSWORDS = {
    "cause_fail": {"roles": ["capped_role"], "settings": {"max_threads": "16"}},
    "valid": {"settings": {"max_result_rows": "555"}},
}

# limit_user: used by test_named_session_refused_by_session_limit_not_reusable. "password_a"
# returns a role whose profile sets max_sessions_for_user = 1 (so named-session admission can
# be refused) plus a distinctive auth setting; "password_b" returns a role whose profile caps
# max_threads and a different value of the same auth setting, so a session created under
# "password_b" is distinguishable from a leftover of a refused "password_a" creation.
LIMIT_USER_PASSWORDS = {
    "password_a": {"roles": ["limit_role_a"], "settings": {"max_result_rows": "111"}},
    "password_b": {"roles": ["limit_role_b"], "settings": {"max_result_rows": "555"}},
}

# Users known only to node4's directory (default_profile + networks).
# aux_override_user: response settings override the directory profile's value for the
# same setting.
AUX_USERS = {
    "aux_user": {"body": {"roles": []}},
    "aux_override_user": {"body": {"settings": {"max_rows_to_read": "777"}}},
}

# Users known only to node3's directory (cache-bound, configured BEFORE users.xml).
# shadowed_user also exists in users.xml with password "xml_password": the helper
# rejects that password (401), which must fail closed WITHOUT falling through to
# the later users.xml storage.
CACHE_USERS = {f"cache_user_{i}": {"body": {}} for i in range(10)}
CACHE_USERS["shadowed_user"] = {"body": {}}
# truncated_404_user also exists in users.xml (password "local_pw"). node3's helper answers
# 404 with Content-Length: 100 and then closes after 2 bytes: the incomplete response must
# fail the attempt, never fall through to users.xml.
CACHE_USERS["truncated_404_user"] = {"body": {}}

# Framing cases for test_response_framing_contract (all MAIN_USERS, any password accepted):
#   truncated_200_user        200, Content-Length: 100, 2 bytes sent, then close  -> fail
#   close_delimited_empty_user 200, no Content-Length/chunked, empty body, close  -> fail
#   close_delimited_body_user  200, no Content-Length/chunked, body "{}", close   -> fail
#   content_length_zero_user   200, Content-Length: 0 (ordinary _reply with "")    -> ok
FRAMING_USERS = {
    "truncated_200_user",
    "close_delimited_empty_user",
    "close_delimited_body_user",
}


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _decode_basic_auth(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            return None, None
        user, _, password = base64.b64decode(auth[6:]).decode().partition(":")
        return user, password

    def _reply_raw(self, status, headers, payload, close_after=True):
        # Writes a response with explicit framing headers (and no automatic Content-Length),
        # then closes the connection so the client observes exactly this framing.
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.end_headers()
        if payload:
            self.wfile.write(payload)
        self.wfile.flush()
        if close_after:
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()

    def _reply(self, status, body=""):
        payload = body if isinstance(body, str) else json.dumps(body)
        data = payload.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _auth_against(self, users):
        user, password = self._decode_basic_auth()
        SEEN_USERS.add(user)  # record that the directory actually consulted this server
        REQUEST_COUNTS[user] += 1
        if user == "expiry_user" and users is MAIN_USERS:
            # password format: "until_<absolute_epoch>"; returns that valid_until and the
            # reader role. Absolute, so a repeated auth returns the SAME deadline.
            if password.startswith("until_"):
                return self._reply(
                    200,
                    {
                        "valid_until": int(password[len("until_") :]),
                        "roles": ["reader"],
                    },
                )
            return self._reply(401)
        if user == "dual_user" and users is MAIN_USERS:
            if password in DUAL_USER_PASSWORDS:
                return self._reply(200, DUAL_USER_PASSWORDS[password])
            return self._reply(401)
        if user == "probe_user" and users is MAIN_USERS:
            if password in PROBE_USER_PASSWORDS:
                return self._reply(200, PROBE_USER_PASSWORDS[password])
            return self._reply(401)
        if user == "guard_user" and users is MAIN_USERS:
            if password in GUARD_USER_PASSWORDS:
                return self._reply(200, GUARD_USER_PASSWORDS[password])
            return self._reply(401)
        if user == "limit_user" and users is MAIN_USERS:
            if password in LIMIT_USER_PASSWORDS:
                return self._reply(200, LIMIT_USER_PASSWORDS[password])
            return self._reply(401)
        if user == "truncated_200_user" and users is MAIN_USERS:
            return self._reply_raw(200, [("Content-Length", "100")], b"{}")
        if user == "close_delimited_empty_user" and users is MAIN_USERS:
            return self._reply_raw(200, [("Connection", "close")], b"")
        if user == "close_delimited_body_user" and users is MAIN_USERS:
            return self._reply_raw(200, [("Connection", "close")], b"{}")
        if user == "truncated_404_user" and users is CACHE_USERS:
            return self._reply_raw(404, [("Content-Length", "100")], b"{}")
        if user == "conn_reset_user" and users is MAIN_USERS:
            # Simulate a transport failure (connection closed without any HTTP response)
            # so the client sees this as a retryable failure, unlike a parsed response.
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if user not in users:
            return self._reply(404)
        if password != GOOD_PASSWORD:
            if user == "body401_user":
                return self._reply(401, {"error": "wrong password"})
            return self._reply(401)
        entry = users[user]
        if user.startswith("barrier_user_"):
            try:
                CONCURRENT_BARRIER.wait()
            except threading.BrokenBarrierError:
                return self._reply(503)
        return self._reply(entry.get("status", 200), entry.get("body", ""))

    def do_GET(self):
        if self.path == "/health":
            return self._reply(200, "OK")
        # Debug endpoint: was a username ever presented to this server? Used to prove the
        # http directory was actually consulted (precedence), not bypassed by users.xml.
        if self.path.startswith("/seen?"):
            from urllib.parse import parse_qs, urlparse

            wanted = parse_qs(urlparse(self.path).query).get("user", [""])[0]
            return self._reply(200, "1" if wanted in SEEN_USERS else "0")
        # Debug endpoint: how many authentication requests has this server received for
        # a username? Used to prove that a parsed HTTP response is never retried, while a
        # transport failure is retried up to max_tries.
        if self.path.startswith("/count?"):
            from urllib.parse import parse_qs, urlparse

            wanted = parse_qs(urlparse(self.path).query).get("user", [""])[0]
            return self._reply(200, str(REQUEST_COUNTS[wanted]))
        if self.path == "/main":
            return self._auth_against(MAIN_USERS)
        if self.path == "/aux":
            return self._auth_against(AUX_USERS)
        if self.path == "/cache":
            return self._auth_against(CACHE_USERS)
        return self._reply(404)


if __name__ == "__main__":
    # ThreadingHTTPServer (not the single-threaded HTTPServer) so the concurrency test
    # can prove that different usernames authenticate concurrently — a single-threaded
    # server would serialize requests at the mock and mask a directory-wide lock.
    http.server.ThreadingHTTPServer(("0.0.0.0", 8000), RequestHandler).serve_forever()
