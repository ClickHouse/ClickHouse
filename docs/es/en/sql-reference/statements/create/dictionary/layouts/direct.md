---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'layout de diccionario direct'
sidebar_label: 'direct'
sidebar_position: 9
description: 'Un layout de diccionario que consulta la fuente directamente sin caché.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

El diccionario no se almacena en memoria, sino que accede directamente a la fuente al procesar una solicitud.

La clave del diccionario es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

Se admiten todos los tipos de [fuentes](../sources/#dictionary-sources), excepto los archivos locales.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

Este tipo de almacenamiento se utiliza con [claves compuestas](../attributes.md#composite-key). Similar a `direct`.