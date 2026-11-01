const SettingsInfoBlock = ({ type, default_value, changeable_without_restart }) => {
  return (
    <div
      className="not-prose"
      style={{
        display: "flex",
        flexWrap: "wrap",
        alignItems: "baseline",
        columnGap: "0.5rem",
        rowGap: "0.125rem",
        margin: "0.375rem 0",
        fontSize: "0.8125rem",
        lineHeight: "1.125rem",
      }}
    >
      <div style={{ fontWeight: 600, opacity: 0.72 }}>类型</div>
      <div style={{ overflowWrap: "anywhere" }}>{type}</div>
      <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>默认值</div>
      <div style={{ overflowWrap: "anywhere" }}>{default_value}</div>
      {changeable_without_restart && (
        <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>
          无需重启即可更改
        </div>
      )}
      {changeable_without_restart && (
        <div style={{ overflowWrap: "anywhere" }}>
          {changeable_without_restart}
        </div>
      )}
    </div>
  );
};
export default SettingsInfoBlock;