---
description: 'TCP 연결 제한.'
sidebar_label: 'TCP 연결 제한'
slug: /operations/settings/tcp-connection-limits
title: 'TCP 연결 제한'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

ClickHouse TCP 연결(즉, [command-line client](https://clickhouse.com/docs/interfaces/client)를 통한 연결)은 일정 횟수의 쿼리 실행 후 또는 일정 시간이 지나면 자동으로 연결이 끊어질 수 있습니다.
연결이 끊어진 후에는 자동으로 재연결되지 않습니다(다만 다른 방식으로 트리거되는 경우는 예외이며,
예를 들어 command-line client에서 다른 쿼리를 보내는 경우가 그렇습니다).

연결 제한은 서버 설정인
`tcp_close_connection_after_queries_num`(쿼리 수 제한)
또는 `tcp_close_connection_after_queries_seconds`(지속 시간 제한)을 0보다 크게 설정하면 활성화됩니다.
두 제한이 모두 활성화된 경우에는 둘 중 먼저 도달한 제한에 따라 연결이 종료됩니다.

제한에 도달해 연결이 끊어지면, 클라이언트는
`TCP_CONNECTION_LIMIT_REACHED` 예외를 수신하며, **연결 종료를 유발한 쿼리는 절대 처리되지 않습니다**.

<div id="query-limits">
  ## 쿼리 제한
</div>

`tcp_close_connection_after_queries_num`가 N으로 설정되어 있다고 가정하면, 연결에서는
성공한 쿼리 N개까지 허용됩니다. 그리고 N + 1번째 쿼리에서 클라이언트 연결이 끊어집니다.

처리된 모든 쿼리는 쿼리 제한에 포함됩니다. 따라서 command-line client로 연결할 때,
초기 시스템 경고 쿼리가 자동으로 실행되어 이 제한에 포함될 수 있습니다.

TCP 연결이 idle 상태일 때(즉, 세션 설정 `poll_interval`로 지정된 일정 시간 동안
쿼리를 처리하지 않았을 때), 지금까지 집계된 쿼리 수는 0으로 재설정됩니다.
즉, idle 상태가 발생하면 단일 연결의 총 쿼리 수가
`tcp_close_connection_after_queries_num`를 초과할 수 있습니다.

<div id="duration-limits">
  ## 지속 시간 제한
</div>

연결 지속 시간은 클라이언트가 연결되는 즉시부터 측정됩니다.
`tcp_close_connection_after_queries_seconds`초가 경과한 후 처음 실행되는 쿼리에서 클라이언트 연결이 종료됩니다.