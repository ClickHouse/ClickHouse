---
description: 'Guía para compilar ClickHouse desde el código fuente para la arquitectura AARCH64'
sidebar_label: 'Compilar en Linux para AARCH64'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'Cómo compilar ClickHouse en Linux para AARCH64'
doc_type: 'guide'
---

No se requieren pasos especiales para compilar ClickHouse para Aarch64 en una máquina Aarch64.

Para compilar ClickHouse de forma cruzada para AArch64 en una máquina Linux x86, pase el siguiente indicador a `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`