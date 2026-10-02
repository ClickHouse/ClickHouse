export const SampleDatasetExplorer = ({ categories }) => {
  const ACCENT = "#FAFF69"
  const assetBase = typeof window !== "undefined" && window.location.pathname.startsWith("/docs") ? "/docs" : ""
  const withBase = (p) => (p && p.startsWith("/") ? assetBase + p : p)

  // Each category: id, title (shown beneath the banner image), an icon used for
  // its child cards, the two banner images, and the child dataset pages.
  const CATEGORIES = [
    {
      id: "benchmarks",
      title: "基准测试",
      icon: "gauge",
      imgLight: "/images/sample-datasets-grid/benchmarks-light.jpg",
      imgDark: "/images/sample-datasets-grid/benchmarks-dark.jpg",
      datasets: [
        {
          title: "AMPLab Big Data Benchmark",
          href: "/zh/get-started/sample-datasets/amplab-benchmark",
          imgLight: "/images/sample-datasets-grid/amplab-benchmark-light.jpg",
          imgDark: "/images/sample-datasets-grid/amplab-benchmark-dark.jpg"
        },
        {
          title: "Brown University Benchmark",
          href: "/zh/get-started/sample-datasets/brown-benchmark",
          imgLight: "/images/sample-datasets-grid/brown-benchmark-light.jpg",
          imgDark: "/images/sample-datasets-grid/brown-benchmark-dark.jpg"
        },
        {
          title: "Star Schema Benchmark (SSB)",
          href: "/zh/get-started/sample-datasets/star-schema",
          imgLight: "/images/sample-datasets-grid/star-schema-light.jpg",
          imgDark: "/images/sample-datasets-grid/star-schema-dark.jpg"
        },
        { title: "TPC-DS", href: "/zh/get-started/sample-datasets/tpcds", imgLight: "/images/sample-datasets-grid/tpcds-light.jpg", imgDark: "/images/sample-datasets-grid/tpcds-dark.jpg" },
        { title: "TPC-H", href: "/zh/get-started/sample-datasets/tpch", imgLight: "/images/sample-datasets-grid/tpch-light.jpg", imgDark: "/images/sample-datasets-grid/tpch-dark.jpg" }
      ]
    },
    {
      id: "geo-location",
      title: "地理与位置",
      icon: "map-pin",
      imgLight: "/images/sample-datasets-grid/geo-location-light.jpg",
      imgDark: "/images/sample-datasets-grid/geo-location-dark.jpg",
      datasets: [
        {
          title: "蜂窝基站（OpenCelliD）",
          href: "/zh/get-started/sample-datasets/cell-towers",
          imgLight: "/images/sample-datasets-grid/cell-towers-light.jpg",
          imgDark: "/images/sample-datasets-grid/cell-towers-dark.jpg"
        },
        {
          title: "Foursquare 地点",
          href: "/zh/get-started/sample-datasets/foursquare-os-places",
          imgLight: "/images/sample-datasets-grid/foursquare-places-light.jpg",
          imgDark: "/images/sample-datasets-grid/foursquare-places-dark.jpg"
        },
        {
          title: "纽约出租车数据",
          href: "/zh/get-started/sample-datasets/nyc-taxi",
          imgLight: "/images/sample-datasets-grid/nyc-taxi-light.jpg",
          imgDark: "/images/sample-datasets-grid/nyc-taxi-dark.jpg"
        }
      ]
    },
    {
      id: "public-records",
      title: "公共记录与开放数据",
      icon: "landmark",
      imgLight: "/images/sample-datasets-grid/public-records-light.jpg",
      imgDark: "/images/sample-datasets-grid/public-records-dark.jpg",
      datasets: [
        {
          title: "COVID-19 开放数据",
          href: "/zh/get-started/sample-datasets/covid19",
          imgLight: "/images/sample-datasets-grid/covid19-light.jpg",
          imgDark: "/images/sample-datasets-grid/covid19-dark.jpg"
        },
        {
          title: "纽约警察局投诉数据",
          href: "/zh/get-started/sample-datasets/nypd-complaint-data",
          imgLight: "/images/sample-datasets-grid/nypd-complaint-data-light.jpg",
          imgDark: "/images/sample-datasets-grid/nypd-complaint-data-dark.jpg"
        },
        {
          title: "OnTime（航班数据）",
          href: "/zh/get-started/sample-datasets/ontime",
          imgLight: "/images/sample-datasets-grid/ontime-light.jpg",
          imgDark: "/images/sample-datasets-grid/ontime-dark.jpg"
        },
        {
          title: "英国房产价格",
          href: "/zh/get-started/sample-datasets/uk-price-paid",
          imgLight: "/images/sample-datasets-grid/uk-price-paid-light.jpg",
          imgDark: "/images/sample-datasets-grid/uk-price-paid-dark.jpg"
        },
        {
          title: "菜单上有什么？（NYPL）",
          href: "/zh/get-started/sample-datasets/menus",
          imgLight: "/images/sample-datasets-grid/menus-light.jpg",
          imgDark: "/images/sample-datasets-grid/menus-dark.jpg"
        }
      ]
    },
    {
      id: "time-series-sensors",
      title: "时间序列与传感器",
      icon: "activity",
      imgLight: "/images/sample-datasets-grid/time-series-sensors-light.jpg",
      imgDark: "/images/sample-datasets-grid/time-series-sensors-dark.jpg",
      datasets: [
        {
          title: "环境传感器数据",
          href: "/zh/get-started/sample-datasets/environmental-sensors",
          imgLight: "/images/sample-datasets-grid/environmental-sensors-light.jpg",
          imgDark: "/images/sample-datasets-grid/environmental-sensors-dark.jpg"
        },
        {
          title: "NOAA 全球历史气候网络",
          href: "/zh/get-started/sample-datasets/noaa",
          imgLight: "/images/sample-datasets-grid/noaa-light.jpg",
          imgDark: "/images/sample-datasets-grid/noaa-dark.jpg"
        },
        {
          title: "台湾历史天气",
          href: "/zh/get-started/sample-datasets/tw-weather",
          imgLight: "/images/sample-datasets-grid/tw-weather-light.jpg",
          imgDark: "/images/sample-datasets-grid/tw-weather-dark.jpg"
        }
      ]
    },
    {
      id: "vector-search",
      title: "向量搜索与嵌入向量",
      icon: "search",
      imgLight: "/images/sample-datasets-grid/vector-search-light.jpg",
      imgDark: "/images/sample-datasets-grid/vector-search-dark.jpg",
      datasets: [
        {
          title: "dbpedia dataset",
          href: "/zh/get-started/sample-datasets/dbpedia",
          imgLight: "/images/sample-datasets-grid/dbpedia-light.jpg",
          imgDark: "/images/sample-datasets-grid/dbpedia-dark.jpg"
        },
        {
          title: "Hacker News 向量搜索",
          href: "/zh/get-started/sample-datasets/hacker-news-vector-search",
          imgLight: "/images/sample-datasets-grid/hacker-news-vector-search-light.jpg",
          imgDark: "/images/sample-datasets-grid/hacker-news-vector-search-dark.jpg"
        },
        {
          title: "LAION 5B dataset",
          href: "/zh/get-started/sample-datasets/laion5b",
          imgLight: "/images/sample-datasets-grid/laion5b-light.jpg",
          imgDark: "/images/sample-datasets-grid/laion5b-dark.jpg"
        },
        {
          title: "Laion-400M dataset",
          href: "/zh/get-started/sample-datasets/laion",
          imgLight: "/images/sample-datasets-grid/laion-400m-light.jpg",
          imgDark: "/images/sample-datasets-grid/laion-400m-dark.jpg"
        }
      ]
    },
    {
      id: "web-social",
      title: "网络与社交分析",
      icon: "globe",
      imgLight: "/images/sample-datasets-grid/web-social-analytics-light.jpg",
      imgDark: "/images/sample-datasets-grid/web-social-analytics-dark.jpg",
      datasets: [
        {
          title: "亚马逊用户评论",
          href: "/zh/get-started/sample-datasets/amazon-reviews",
          imgLight: "/images/sample-datasets-grid/amazon-reviews-light.jpg",
          imgDark: "/images/sample-datasets-grid/amazon-reviews-dark.jpg"
        },
        {
          title: "分析 Stack Overflow 数据",
          href: "/zh/get-started/sample-datasets/stackoverflow",
          imgLight: "/images/sample-datasets-grid/stackoverflow-light.jpg",
          imgDark: "/images/sample-datasets-grid/stackoverflow-dark.jpg"
        },
        {
          title: "匿名网站分析",
          href: "/zh/get-started/sample-datasets/anon-web-analytics-metrica",
          imgLight: "/images/sample-datasets-grid/anon-web-analytics-light.jpg",
          imgDark: "/images/sample-datasets-grid/anon-web-analytics-dark.jpg"
        },
        {
          title: "Criteo TB 级点击日志",
          href: "/zh/get-started/sample-datasets/criteo",
          imgLight: "/images/sample-datasets-grid/criteo-light.jpg",
          imgDark: "/images/sample-datasets-grid/criteo-dark.jpg"
        },
        {
          title: "GitHub 事件数据集",
          href: "/zh/get-started/sample-datasets/github-events",
          imgLight: "/images/sample-datasets-grid/github-events-light.jpg",
          imgDark: "/images/sample-datasets-grid/github-events-dark.jpg"
        },
        {
          title: "Hacker News dataset",
          href: "/zh/get-started/sample-datasets/hacker-news",
          imgLight: "/images/sample-datasets-grid/hacker-news-light.jpg",
          imgDark: "/images/sample-datasets-grid/hacker-news-dark.jpg"
        },
        {
          title: "查询 GitHub 数据",
          href: "/zh/get-started/sample-datasets/github",
          imgLight: "/images/sample-datasets-grid/github-light.jpg",
          imgDark: "/images/sample-datasets-grid/github-dark.jpg"
        },
        { title: "WikiStat", href: "/zh/get-started/sample-datasets/wikistat", imgLight: "/images/sample-datasets-grid/wikistat-light.jpg", imgDark: "/images/sample-datasets-grid/wikistat-dark.jpg" },
        {
          title: "YouTube 点踩数据集",
          href: "/zh/get-started/sample-datasets/youtube-dislikes",
          imgLight: "/images/sample-datasets-grid/youtube-dislikes-light.jpg",
          imgDark: "/images/sample-datasets-grid/youtube-dislikes-dark.jpg"
        }
      ]
    }
  ]

  const cats = categories || CATEGORIES

  const [selectedId, setSelectedId] = useState(null)
  const selected = cats.find((c) => c.id === selectedId) || null

  // Theme visibility is handled by explicit `.dark` descendant selectors in the
  // <style> block below (Mintlify's class strategy — same approach as
  // IntegrationGrid). Tailwind `dark:` utilities are NOT reliable here: they
  // compile against the OS media query, so they'd ignore the in-app light/dark
  // toggle. Note the reversed-colour scheme: light mode shows the *dark* (black)
  // banner art, dark mode shows the *light* (yellow) art.
  const Banner = ({ cat, className }) => (
    <>
      <img className={`sde-img-dark ${className || ""}`} src={withBase(cat.imgDark)} alt={cat.title} />
      <img className={`sde-img-light ${className || ""}`} src={withBase(cat.imgLight)} alt={cat.title} />
    </>
  )

  return (
    <div className="sde-root my-8">
      <style
        dangerouslySetInnerHTML={{
          __html: `
        @keyframes sde-pop {
          from { opacity: 0; transform: translateY(14px) scale(0.96); }
          to   { opacity: 1; transform: translateY(0) scale(1); }
        }
        @keyframes sde-fade {
          from { opacity: 0; }
          to   { opacity: 1; }
        }
        .sde-view { animation: sde-fade 0.25s ease both; }
        /* Reversed scheme: dark (black) art in light mode, light (yellow) art in dark mode.
           Use explicit .dark selectors — Tailwind dark: utilities follow the OS here. */
        .sde-root .sde-img-dark { display: block; }
        .sde-root .sde-img-light { display: none; }
        .dark .sde-root .sde-img-dark { display: none; }
        .dark .sde-root .sde-img-light { display: block; }
        .sde-tile {
          display: block;
          width: 100%;
          padding: 0;
          border: none;
          background: transparent;
          text-align: left;
          cursor: pointer;
          animation: sde-pop 0.4s cubic-bezier(0.22, 1, 0.36, 1) both;
          transition: transform 0.25s cubic-bezier(0.22, 1, 0.36, 1);
        }
        .sde-tile:hover { transform: translateY(-4px) scale(1.015); }
        .sde-tile:active { transform: translateY(-1px) scale(0.995); }
        /* Border matches the HeroCard cards on index.mdx:
           border-[#e5e7eb] dark:border-[#3c3c3c], 8px radius */
        .sde-tile-media {
          display: block;
          position: relative;
          aspect-ratio: 4 / 3;
          border: 1px solid #e5e7eb;
          border-radius: 8px;
          overflow: hidden;
          box-shadow: 0 1px 3px rgba(0,0,0,0.12);
          transition: box-shadow 0.25s ease;
        }
        .dark .sde-root .sde-tile-media { border-color: #3c3c3c; }
        .sde-tile:hover .sde-tile-media { box-shadow: 0 12px 28px rgba(0,0,0,0.22); }
        .sde-tile img {
          width: 100%;
          height: 100%;
          object-fit: cover;
          margin: 0;
          pointer-events: none;
        }
        /* hover hint: translucent strip along the bottom of the image */
        .sde-tile-hint {
          position: absolute;
          left: 0;
          right: 0;
          bottom: 0;
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
          padding: 10px 14px;
          background: rgba(0,0,0,0.45);
          backdrop-filter: blur(6px);
          -webkit-backdrop-filter: blur(6px);
          opacity: 0;
          transition: opacity 0.25s ease;
          pointer-events: none;
        }
        .sde-tile:hover .sde-tile-hint { opacity: 1; }
        @media (max-width: 639px), (any-hover: none) {
          .sde-tile-hint { opacity: 1; }
        }
        .sde-tile-title {
          display: block;
          margin-top: 0.65rem;
          font-size: 0.95rem;
          font-weight: 600;
          line-height: 1.3;
          color: inherit;
        }
        .sde-count {
          font-size: 0.78rem;
          font-weight: 600;
          color: #fff;
        }
        .sde-explore {
          font-size: 0.78rem;
          font-weight: 700;
          color: ${ACCENT};
          display: inline-flex;
          align-items: center;
          gap: 4px;
        }
        .sde-child { animation: sde-pop 0.45s cubic-bezier(0.22, 1, 0.36, 1) both; }
        .sde-back {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          cursor: pointer;
          background: transparent;
          border: none;
          padding: 0;
          color: inherit;
          opacity: 0.5;
          font-size: 0.875rem;
          font-weight: 500;
          transition: opacity 0.2s ease;
        }
        .sde-back:hover { opacity: 1; }
        .sde-detail-title {
          font-size: 1.5rem;
          font-weight: 600;
          line-height: 1.3;
          margin: 0 0 1.25rem 0;
          animation: sde-pop 0.4s cubic-bezier(0.22, 1, 0.36, 1) both;
        }
      `
        }}
      />

      {!selected ? (
        <div className="sde-view">
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-6 items-start">
            {cats.map((cat, i) => (
              <button key={cat.id} type="button" className="sde-tile" style={{ animationDelay: `${i * 60}ms` }} onClick={() => setSelectedId(cat.id)} aria-label={`Explore ${cat.title} datasets`}>
                <span className="sde-tile-media">
                  <Banner cat={cat} />
                  <span className="sde-tile-hint">
                    <span className="sde-count">
                      {cat.datasets.length} 个数据集
                    </span>
                    <span className="sde-explore">
                      Explore
                      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M6 4l4 4-4 4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </span>
                  </span>
                </span>
                <span className="sde-tile-title">{cat.title}</span>
              </button>
            ))}
          </div>
        </div>
      ) : (
        <div className="sde-view" key={selected.id}>
          <div className="mb-6">
            <button type="button" className="sde-back" onClick={() => setSelectedId(null)}>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M15 18l-6-6 6-6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              All categories
            </button>
          </div>

          <h2 className="sde-detail-title">{selected.title}</h2>

          <div className="grid grid-cols-2 lg:grid-cols-3 gap-6 items-start">
            {selected.datasets.map((ds, i) => (
              <a key={ds.href} href={ds.href} className="sde-child sde-tile" style={{ animationDelay: `${i * 50}ms` }}>
                <span className="sde-tile-media">
                  {ds.imgDark && <img className="sde-img-dark" src={withBase(ds.imgDark)} alt={ds.title} />}
                  {ds.imgLight && <img className="sde-img-light" src={withBase(ds.imgLight)} alt={ds.title} />}
                  <span className="sde-tile-hint">
                    <span className="sde-explore">
                      查看数据集
                      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M6 4l4 4-4 4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </span>
                  </span>
                </span>
                <span className="sde-tile-title">{ds.title}</span>
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
