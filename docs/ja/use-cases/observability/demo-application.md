---
title: デモアプリケーション
description: 観測性のためのデモアプリケーション
slug: /ja/observability/demo-application
keywords: [観測性, ログ, トレース, メトリクス, OpenTelemetry, Grafana, otel]
---

Open Telemetry プロジェクトには、[デモアプリケーション](https://opentelemetry.io/docs/demo/)が含まれています。このアプリケーションのフォーク版で、ClickHouse をログとトレースのデータソースとして活用しているものが[こちら](https://github.com/ClickHouse/opentelemetry-demo)にあります。公式の[デモ手順](https://opentelemetry.io/docs/demo/docker-deployment/)に従って、このデモを Docker でデプロイできます。既存の[コンポーネント](https://opentelemetry.io/docs/demo/collector-data-flow-dashboard/)に加えて、ClickHouse のインスタンスがデプロイされ、ログとトレースのストレージとして使用されます。
