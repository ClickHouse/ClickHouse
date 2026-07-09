---
description: 'mecanismo de caché que permite almacenar datos en caché en memoria en el mismo proceso, en lugar de depender de la caché de páginas del SO.'
sidebar_label: 'Caché de páginas en espacio de usuario'
sidebar_position: 65
slug: /operations/userspace-page-cache
title: 'Caché de páginas en espacio de usuario'
doc_type: 'referencia'
---

<div id="overview">
  ## Descripción general
</div>

> La caché de páginas en espacio de usuario es un nuevo mecanismo de caché que permite almacenar
> datos en memoria en el mismo proceso en lugar de depender de la caché de páginas del SO.

ClickHouse ya ofrece la [caché del sistema de archivos](/es/docs/operations/storing-data)
como una forma de caché sobre almacenamiento remoto de objetos, como Amazon S3, Google
Cloud Storage (GCS) o Azure Blob Storage. La caché de páginas en espacio de usuario está diseñada
para acelerar el acceso a datos remotos cuando la caché normal del SO no resulta suficiente.

Se diferencia de la caché del sistema de archivos en los siguientes aspectos:

| Caché del sistema de archivos                                         | Caché de páginas en espacio de usuario    |
| --------------------------------------------------------------------- | ----------------------------------------- |
| Escribe datos en el sistema de archivos local                         | Presente solo en memoria                  |
| Ocupa espacio en disco (también configurable en tmpfs)                | Independiente del sistema de archivos     |
| Sobrevive a los reinicios del servidor                                | No sobrevive a los reinicios del servidor |
| No aparece en el uso de memoria del servidor                          | Aparece en el uso de memoria del servidor |
| Adecuada tanto para disco como para memoria (caché de páginas del SO) | **Buena para servidores sin disco**       |

<div id="configuration-settings-and-usage">
  ## Ajustes de configuración y uso
</div>

<div id="usage">
  ### Uso
</div>

Para habilitar la caché de páginas en espacio de usuario, primero configúrela en el servidor:

```bash
cat config.d/page_cache.yaml
page_cache_max_size: 100G
```

:::note
La caché de páginas en espacio de usuario usará hasta la cantidad de memoria especificada, pero
esa cantidad de memoria no queda reservada. La memoria se desalojará cuando sea necesaria
para otras necesidades del servidor.
:::

A continuación, habilite su uso a nivel de consulta:

```sql
SET use_page_cache_for_disks_without_file_cache=1;
```

<div id="settings">
  ### Configuración
</div>

| Configuración                                           | Descripción                                                                                                                                                                                                                                                                                                                                                                                               | Predeterminado |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `use_page_cache_for_disks_without_file_cache`           | Usa la caché de páginas en espacio de usuario para discos remotos que no tengan habilitada la caché del sistema de archivos.                                                                                                                                                                                                                                                                              | `0`            |
| `use_page_cache_with_distributed_cache`                 | Usa la caché de páginas en espacio de usuario cuando se utilice distributed cache.                                                                                                                                                                                                                                                                                                                        | `0`            |
| `read_from_page_cache_if_exists_otherwise_bypass_cache` | Usa la caché de páginas en espacio de usuario en modo pasivo, de forma similar a [`read_from_filesystem_cache_if_exists_otherwise_bypass_cache`](/es/docs/operations/settings/settings#read_from_filesystem_cache_if_exists_otherwise_bypass_cache).                                                                                                                                                         | `0`            |
| `page_cache_inject_eviction`                            | La caché de páginas en espacio de usuario invalidará ocasionalmente algunas páginas de forma aleatoria. Pensado para pruebas.                                                                                                                                                                                                                                                                             | `0`            |
| `page_cache_block_size`                                 | Tamaño de los fragmentos de archivo que se almacenarán en la caché de páginas en espacio de usuario, en bytes. Todas las lecturas que pasen por la caché se redondearán al múltiplo superior de este tamaño.                                                                                                                                                                                              | `1048576`      |
| `page_cache_history_window_ms`                          | Retraso antes de que la memoria liberada pueda ser utilizada por la caché de páginas en espacio de usuario.                                                                                                                                                                                                                                                                                               | `1000`         |
| `page_cache_policy`                                     | Nombre de la política de la caché de páginas en espacio de usuario.                                                                                                                                                                                                                                                                                                                                       | `SLRU`         |
| `page_cache_size_ratio`                                 | Tamaño de la cola protegida de la caché de páginas en espacio de usuario en relación con el tamaño total de la caché.                                                                                                                                                                                                                                                                                     | `0.5`          |
| `page_cache_min_size`                                   | Tamaño mínimo de la caché de páginas en espacio de usuario.                                                                                                                                                                                                                                                                                                                                               | `104857600`    |
| `page_cache_max_size`                                   | Tamaño máximo de la caché de páginas en espacio de usuario. Establécelo en 0 para deshabilitar la caché. Si es mayor que page&#95;cache&#95;min&#95;size, el tamaño de la caché se ajustará continuamente dentro de este rango para usar la mayor parte de la memoria disponible, manteniendo al mismo tiempo el uso total de memoria por debajo del límite (`max_server_memory_usage`[`_to_ram_ratio`]). | `0`            |
| `page_cache_free_memory_ratio`                          | Fracción del límite de memoria que debe mantenerse libre de la caché de páginas en espacio de usuario. Equivalente a la configuración min&#95;free&#95;kbytes de Linux.                                                                                                                                                                                                                                   | `0.15`         |
| `page_cache_lookahead_blocks`                           | Si se produce un fallo de caché en la caché de páginas en espacio de usuario, lee de una sola vez hasta esta cantidad de bloques consecutivos desde el almacenamiento subyacente, si tampoco están en la caché. Cada bloque tiene un tamaño de page&#95;cache&#95;block&#95;size bytes.                                                                                                                   | `16`           |
| `page_cache_shards`                                     | Distribuye la caché de páginas en espacio de usuario entre esta cantidad de segmentos para reducir la contención de mutex. Experimental; no es probable que mejore el rendimiento.                                                                                                                                                                                                                        | `4`            |

<div id="related-content">
  ## Contenido relacionado
</div>

* [Caché del sistema de archivos](/es/docs/operations/storing-data)
* [Seminario web sobre el lanzamiento de ClickHouse v25.3](https://www.youtube.com/live/iCKEzp0_Z2Q?feature=shared\&t=1320)