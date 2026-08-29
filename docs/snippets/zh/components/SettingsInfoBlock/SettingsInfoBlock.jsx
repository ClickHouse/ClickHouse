const SettingsInfoBlock = ({ type, default_value, changeable_without_restart }) => {
  const cells = [
    ["类型", <Badge color="surface">{type}</Badge>],
    ["默认值", <Badge color="surface">{default_value}</Badge>],
  ];
  if (changeable_without_restart) {
    const isYes = String(changeable_without_restart).trim().toLowerCase() === "yes";
    const badge = isYes ? (
      <Badge icon="check" stroke color="green" size="sm">是</Badge>
    ) : (
      <Badge icon="x" stroke color="red" size="sm">否</Badge>
    );
    cells.push(["可在不重启的情况下更改", badge]);
  }
  return (
    <table>
      <thead>
        <tr>
          {cells.map(([h]) => (
            <th key={h}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        <tr>
          {cells.map(([h, v]) => (
            <td key={h}>{v}</td>
          ))}
        </tr>
      </tbody>
    </table>
  );
};
export default SettingsInfoBlock;