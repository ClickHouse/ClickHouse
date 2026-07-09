---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'مصدر القاموس: YTsaurus'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'تهيئة YTsaurus كمصدر للقاموس في ClickHouse.'
doc_type: 'مرجع'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
هذه ميزة تجريبية قد تتغير في الإصدارات المستقبلية بشكل غير متوافق مع الإصدارات السابقة.
فعِّل استخدام مصدر القاموس YTsaurus
باستخدام الإعداد [`allow_experimental_ytsaurus_dictionary_source`](/ar/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(YTSAURUS(
        http_proxy_urls 'http://localhost:8000'
        cypress_path '//tmp/test'
        oauth_token 'password'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <source>
        <ytsaurus>
            <http_proxy_urls>http://localhost:8000</http_proxy_urls>
            <cypress_path>//tmp/test</cypress_path>
            <oauth_token>password</oauth_token>
            <check_table_schema>1</check_table_schema>
        </ytsaurus>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| الإعداد           | الوصف                                   |
| ----------------- | --------------------------------------- |
| `http_proxy_urls` | عنوان URL لوكيل HTTP الخاص بـ YTsaurus. |
| `cypress_path`    | مسار Cypress لمصدر الجدول.              |
| `oauth_token`     | رمز OAuth.                              |