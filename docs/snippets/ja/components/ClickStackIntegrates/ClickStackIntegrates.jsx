export const ClickStackIntegrates = ({ signals = [] }) => {
  const list = Array.isArray(signals) ? signals : String(signals).split(",");
  const supported = list.map((s) => String(s).trim().toLowerCase());

  // ClickHouseブランドの黄色（#faff69）に濃い色のテキストを使用。
  // ClickHouseSupportedBadgeと一致し、ライトモードとダークモードで同一です。
  const pillStyle = {
    display: "inline-flex",
    alignItems: "center",
    padding: "0.125rem 0.625rem",
    borderRadius: "9999px",
    fontSize: "0.8125rem",
    fontWeight: 500,
    lineHeight: 1.6,
    color: "#161517",
    backgroundColor: "#faff69",
    border: "1px solid rgba(0, 0, 0, .1)",
    marginRight: "0.5rem",
  };

  return (
    <div style={{ margin: "0 0 1.25rem" }}>
      <span style={{ marginRight: "0.5rem" }}>このガイドで統合するシグナル:</span>
      {supported.includes("logs") && <span style={pillStyle}>ログ</span>}
      {supported.includes("metrics") && <span style={pillStyle}>メトリクス</span>}
      {supported.includes("traces") && <span style={pillStyle}>トレース</span>}
    </div>
  );
};
export default ClickStackIntegrates;