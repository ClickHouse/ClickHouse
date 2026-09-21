const SettingsInfoBlock = ({ type, default_value, changeable_without_restart }) => {
  const cells = [
    ["Tipo", <Badge color="surface">{type}</Badge>],
    ["Valor padrão", <Badge color="surface">{default_value}</Badge>],
  ];
  if (changeable_without_restart) {
    const isYes = String(changeable_without_restart).trim().toLowerCase() === "yes";
    const badge = isYes ? (
      <Badge icon="check" stroke color="green" size="sm">Sim</Badge>
    ) : (
      <Badge icon="x" stroke color="red" size="sm">Não</Badge>
    );
    cells.push(["Pode ser alterado sem reiniciar", badge]);
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