---
description: 'Guia para compilação do ClickHouse a partir do código-fonte para a arquitetura AARCH64'
sidebar_label: 'Compilar no Linux para AARCH64'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'Como compilar o ClickHouse no Linux para AARCH64'
doc_type: 'guide'
---

Não é necessário seguir nenhuma etapa especial para compilar o ClickHouse para AArch64 em uma máquina AArch64.

Para fazer a compilação cruzada do ClickHouse para AArch64 em uma máquina Linux x86, passe a seguinte flag para o `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`