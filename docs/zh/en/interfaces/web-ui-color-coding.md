---
description: '内置 Web SQL UI (`/play`) 中按列进行颜色编码模式的文档'
sidebar_label: 'Web UI 颜色编码'
sidebar_position: 23
slug: /interfaces/web-ui-color-coding
title: 'Web UI 颜色编码'
doc_type: 'reference'
sidebar: false
---

内置的 Web SQL UI (`play.html`，通过任意 ClickHouse HTTP 端口上的 [`/play`](/zh/interfaces/http) 路径提供) 可以为结果单元格着色，便于一眼发现某一列中的模式。每一列都有各自独立且可单独切换的颜色编码模式。

<div id="switching-the-mode">
  ## 切换模式
</div>

每个列标题右侧都会出现一个 🌈 图标。点击它可让该列在可用模式之间依次切换。在带有悬停指针 (鼠标) 的设备上，只有鼠标悬停在标题上时才会显示该图标，以免平时干扰视图；而在触摸设备及其他粗略指针设备上，由于没有悬停操作，该图标会始终显示，方便直接点按。

列提供哪些模式取决于其类型：

* 数值列以及 `Date`/`DateTime`/`Date32`/`DateTime64` 列会按 `bar` → `heatmap` → `categorical` → `none` 的顺序循环切换。
* 其他所有列只会在 `none` 和 `categorical` 之间切换。

默认模式是：数值列为 `bar`，其他所有列 (包括日期和时间列) 为 `none`。

<div id="modes">
  ## 模式
</div>

* **`bar`** — 在单元中绘制一条与值成比例的水平条。对于数值列，条形会从零基线开始增长；对于 `Date`/`DateTime` 列，则会改为覆盖该列的 `min`..`max` 范围，因为零基线对时间戳没有实际意义。
* **`heatmap`** — 用颜色填充整个单元背景，以表示按该列最小值和最大值范围缩放后的值。
* **`categorical`** — 用根据单元值哈希得到的颜色填充单元背景，因此相同的值会显示为相同的颜色，不同的值会显示为不同的颜色。这适用于任何列类型。
* **`none`** — 不进行颜色编码。

`Date`、`DateTime`、`Date32` 和 `DateTime64` 列会根据其时间值着色，并按 UTC 解析，因此色标不受查看者浏览器时区的影响。

`heatmap` 和 `categorical` 的背景色使用 `oklch` 色彩空间，只变化色相，而每个主题的明度和色度保持固定，因此无论在亮色主题还是暗色主题中，单元内的文本都保持清晰可读。即使某一行内容显示为多行，背景也会填满整个单元。

<div id="categorical-emphasis">
  ## 基于选择的 categorical 强调
</div>

在 `categorical` 模式下，选择某个单元后，其他具有相同值的单元会被突出显示：它们会使用更粗的字体以及高对比度的文本颜色 (深色主题下为纯白，浅色主题下为纯黑) 。而被选中的单元本身则不会被突出显示。这样可以轻松看出某个特定值在该列中的其他位置还出现在哪里。

<div id="persistence">
  ## 持久化
</div>

所选模式会按列保存在页面 URL 和浏览器历史记录中，因此重新加载页面、共享链接或前后导航时，这些设置都会得以保留。为使 URL 和历史记录状态尽可能精简，仅存储非默认选项。

<div id="limitations">
  ## 局限性
</div>

* 垂直 (转置) 单行布局不显示颜色编码。
* 对于 `DateTime64(9)`，低于一微秒的差异不会在颜色标度中加以区分，因为这种差异不足以形成有意义的视觉渐变。