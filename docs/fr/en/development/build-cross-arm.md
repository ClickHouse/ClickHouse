---
description: 'Guide pour compiler ClickHouse à partir du code source pour l''architecture AARCH64'
sidebar_label: 'Compiler sur Linux pour AARCH64'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'Comment compiler ClickHouse sur Linux pour AARCH64'
doc_type: 'guide'
---

Aucune étape particulière n’est nécessaire pour compiler ClickHouse pour AArch64 sur une machine AArch64.

Pour effectuer une compilation croisée de ClickHouse pour AArch64 sur une machine Linux x86, passez l’option suivante à `cmake` : `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`